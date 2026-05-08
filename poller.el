;;;-*- lexical-binding: t; -*-
;;; poller.el --- Extensible async polling/data pipeline framework

;; Author: Mathis Albertzard
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1"))
;; Keywords: async, processes, data, polling
;; URL: https://github.com/malbertzard/poller.el

;;; Commentary:

;; poller.el provides a composable async data pipeline framework.
;;
;; A poller job consists of:
;;
;; - trigger    -> decides WHEN the job runs
;; - request    -> fetches data asynchronously
;; - parser     -> transforms raw data
;; - target     -> stores transformed data
;; - on-error   -> handles errors
;;
;; Example:
;;
;;   (defvar my-weather nil)
;;
;;   (poller-define
;;     weather
;;     :trigger (poller-timer-trigger 300)
;;     :request #'my-request
;;     :parser #'json-parse-string
;;     :target (poller-variable-target 'my-weather))
;;
;; Request functions must follow this contract:
;;
;;   (lambda (success error)
;;      ...)
;;
;; Where:
;;
;;   success -> function called with raw result
;;   error   -> function called with error object
;;
;; Example request:
;;
;;   (defun my-request (success error)
;;     (url-retrieve
;;      "https://example.com"
;;      (lambda (_status)
;;        (condition-case err
;;            (progn
;;              (goto-char (point-min))
;;              (re-search-forward "\n\n")
;;              (funcall success
;;                       (buffer-substring (point) (point-max))))
;;          (error
;;           (funcall error err))))))
;;

;;; Code:

(require 'cl-lib)

(defgroup poller nil
  "Async polling/data pipeline framework."
  :group 'applications)

(defcustom poller-default-error-handler
  #'poller-log-error
  "Default error handler."
  :type 'function)

(defvar poller-jobs nil
  "List of registered poller jobs.")

(cl-defstruct poller-job
  name
  trigger
  request
  parser
  target
  on-error
  enabled
  metadata
  handles)

(defun poller-log-error (err)
  "Default logger for ERR."
  (message "[poller] error: %S" err))

(defun poller-register-job (job)
  "Register JOB in `poller-jobs'."
  (setq poller-jobs
        (cl-remove-if
         (lambda (j)
           (equal (poller-job-name j)
                  (poller-job-name job)))
         poller-jobs))
  (push job poller-jobs)
  job)

(defun poller-get-job (name)
  "Return poller job by NAME."
  (cl-find-if
   (lambda (job)
     (equal (poller-job-name job) name))
   poller-jobs))

(defun poller-remove-job (name)
  "Remove poller job by NAME."
  (setq poller-jobs
        (cl-remove-if
         (lambda (job)
           (equal (poller-job-name job) name))
         poller-jobs)))

(defun poller-enable-job (name)
  "Enable poller job NAME."
  (let ((job (poller-get-job name)))
    (when job
      (setf (poller-job-enabled job) t))))

(defun poller-disable-job (name)
  "Disable poller job NAME."
  (let ((job (poller-get-job name)))
    (when job
      (setf (poller-job-enabled job) nil))))

(defun poller-run-job (job)
  "Run JOB asynchronously."
  (when (poller-job-enabled job)
    (condition-case outer-err
        (funcall
         (poller-job-request job)
         ;; success callback
         (lambda (raw-data)
           (condition-case parse-err
               (let* ((parser
                       (or (poller-job-parser job)
                           #'identity))
                      (parsed
                       (funcall parser raw-data))
                      (target
                       (poller-job-target job)))
                 (when target
                   (funcall target parsed)))
             (error
              (funcall
               (or (poller-job-on-error job)
                   poller-default-error-handler)
               parse-err))))
         ;; error callback
         (lambda (err)
           (funcall
            (or (poller-job-on-error job)
                poller-default-error-handler)
            err)))
      (error
       (funcall
        (or (poller-job-on-error job)
            poller-default-error-handler)
        outer-err)))))

(defun poller-start-job (job)
  "Start JOB using its trigger."
  (when-let ((trigger (poller-job-trigger job)))
    (funcall trigger job)))

(defun poller-start-all ()
  "Start all registered poller jobs."
  (interactive)
  (dolist (job poller-jobs)
    (poller-start-job job)))

(defun poller-run-now (name)
  "Immediately run job NAME."
  (interactive
   (list
    (intern
     (completing-read
      "Run job: "
      (mapcar
       (lambda (j)
         (symbol-name (poller-job-name j)))
       poller-jobs)))))

  (let ((job (poller-get-job name)))
    (unless job
      (error "No such job: %S" name))
    (poller-run-job job)))

;;;; -------------------------------------------------------------------
;;;; Trigger Helpers
;;;; -------------------------------------------------------------------

(defun poller-timer-trigger (interval)
  "Create repeating timer trigger using INTERVAL seconds."
  (lambda (job)

    (let ((timer
           (run-at-time
            nil
            interval
            (lambda ()
              (poller-run-job job)))))

      (push timer
            (poller-job-handles job))

      timer)))

(defun poller-idle-trigger (seconds)
  "Create idle trigger after SECONDS."

  (lambda (job)

    (let ((timer
           (run-with-idle-timer
            seconds
            t
            (lambda ()
              (poller-run-job job)))))

      (push timer
            (poller-job-handles job))

      timer)))

(defun poller-hook-trigger (hook)
  "Create trigger that runs on HOOK."

  (lambda (job)
    (let ((fn
           (lambda (&rest _)
             (poller-run-job job))))
      (add-hook hook fn)
      (push (cons hook fn)
            (poller-job-handles job))

      fn)))

;;;; -------------------------------------------------------------------
;;;; Target Helpers
;;;; -------------------------------------------------------------------

(defun poller-variable-target (symbol)
  "Create target setter for SYMBOL."

  (lambda (value)
    (set symbol value)))

(defun poller-hash-table-target (table key)
  "Store value in TABLE under KEY."

  (lambda (value)
    (puthash key value table)))

(defun poller-buffer-target (buffer-name)
  "Insert value into BUFFER-NAME."

  (lambda (value)
    (with-current-buffer
        (get-buffer-create buffer-name)
      (erase-buffer)
      (insert
       (format "%s" value)))))

;;;; -------------------------------------------------------------------
;;;; Request Helpers
;;;; -------------------------------------------------------------------

(defun poller-url-request (url)
  "Create async URL request for URL."

  (require 'url)

  (lambda (success error)
    (url-retrieve
     url

     (lambda (_status)
       (condition-case err
           (progn
             (goto-char (point-min))
             (re-search-forward "\n\n")
             (let ((body
                    (buffer-substring
                     (point)
                     (point-max))))

               (kill-buffer (current-buffer))
               (funcall success body)))

         (error
          (funcall error err)))))))

(defun poller-process-request (command &optional args)
  "Create async process request from COMMAND and ARGS."

  (lambda (success error)
    (let ((buffer
           (generate-new-buffer
            (format "*poller-%s*" command))))

      (make-process
       :name (format "poller-%s" command)
       :buffer buffer
       :command (append (list command) args)
       :sentinel
       (lambda (proc _event)
         (unwind-protect
             (if (= 0 (process-exit-status proc))
                 (with-current-buffer (process-buffer proc)
                   (funcall success
                            (buffer-string)))
               (funcall error
                        (format "Process failed: %s"
                                (process-exit-status proc))))
           (kill-buffer
            (process-buffer proc))))))))

(defun poller-function-request (fn)
  "Wrap synchronous FN into async callback style."

  (lambda (success error)

    (condition-case err

        (funcall success
                 (funcall fn))

      (error
       (funcall error err)))))

;;;; -------------------------------------------------------------------
;;;; Parser Helpers
;;;; -------------------------------------------------------------------

(defun poller-json-parser (raw)
  "Parse RAW json into plist."

  (require 'json)

  (json-parse-string
   raw
   :object-type 'plist
   :array-type 'list))

(defun poller-lines-parser (raw)
  "Split RAW into lines."

  (split-string raw "\n" t))

(defun poller-identity-parser (raw)
  "Return RAW unchanged."

  raw)

;;;; -------------------------------------------------------------------
;;;; Cleanup
;;;; -------------------------------------------------------------------

(defun poller-stop-job (name)
  "Stop timers/hooks associated with NAME."

  (interactive
   (list
    (intern
     (completing-read
      "Stop job: "
      (mapcar
       (lambda (j)
         (symbol-name
          (poller-job-name j)))
       poller-jobs)))))

  (let ((job (poller-get-job name)))

    (unless job
      (error "No such job: %S" name))

    (dolist (handle (poller-job-handles job))

      (cond

       ((timerp handle)
        (cancel-timer handle))

       ((consp handle)
        (remove-hook
         (car handle)
         (cdr handle)))))

    (setf (poller-job-handles job) nil)))

(defun poller-stop-all ()
  "Stop all poller jobs."

  (interactive)

  (dolist (job poller-jobs)
    (poller-stop-job
     (poller-job-name job))))

;;;; -------------------------------------------------------------------
;;;; Macro API
;;;; -------------------------------------------------------------------

(cl-defmacro poller-define (name &key
                                 trigger
                                 request
                                 parser
                                 target
                                 on-error
                                 metadata
                                 disabled)

  "Define and register a poller job."

  `(poller-register-job

    (make-poller-job

     :name ',name
     :trigger ,trigger
     :request ,request
     :parser ,(or parser
                  #'identity)
     :target ,target
     :on-error ,(or on-error
                    'poller-default-error-handler)

     :enabled ,(not disabled)
     :metadata ,metadata)))

;;;; -------------------------------------------------------------------
;;;; Example
;;;; -------------------------------------------------------------------

;; (defvar my-ip-data nil)
;;
;; (poller-define
;;   ip-fetcher
;;
;;   :trigger
;;   (poller-timer-trigger 60)
;;
;;   :request
;;   (poller-url-request
;;    "https://api.ipify.org?format=json")
;;
;;   :parser
;;   #'poller-json-parser
;;
;;   :target
;;   (poller-variable-target
;;    'my-ip-data))
;;
;; (poller-start-all)

(provide 'poller)

;;; poller.el ends here

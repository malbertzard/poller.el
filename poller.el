;;; poller.el --- Extensible async polling/data pipeline framework -*- lexical-binding: t; -*-

;; Author: Mathis Albertzard
;; Version: 0.2.0
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
;; Request functions follow this contract:
;;
;;   (lambda (success error)
;;      ...)
;;
;; Where:
;;
;;   success -> function called with raw result
;;   error   -> function called with error object

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'url)

(defgroup poller nil
  "Async polling/data pipeline framework."
  :group 'applications)

(defcustom poller-default-error-handler
  #'poller-log-error
  "Default error handler."
  :type 'function)

(defvar poller-jobs nil
  "List of registered poller jobs.")

;;;; -------------------------------------------------------------------
;;;; Hooks
;;;; -------------------------------------------------------------------

(defcustom poller-before-run-hook nil
  "Hook run before a poller job starts.

Each function receives:
  JOB"
  :type 'hook)

(defcustom poller-after-success-hook nil
  "Hook run after successful completion.

Each function receives:
  JOB PARSED-DATA"
  :type 'hook)

(defcustom poller-after-error-hook nil
  "Hook run after a job errors.

Each function receives:
  JOB ERROR"
  :type 'hook)

(defcustom poller-after-finish-hook nil
  "Hook run when a job finishes regardless of outcome.

Each function receives:
  JOB"
  :type 'hook)

;;;; -------------------------------------------------------------------
;;;; Struct
;;;; -------------------------------------------------------------------

(cl-defstruct poller-job
  name
  trigger
  request
  parser
  target
  on-error
  enabled
  metadata
  handles
  running
  started
  last-run
  last-success
  last-error
  last-result)

;;;; -------------------------------------------------------------------
;;;; Logging
;;;; -------------------------------------------------------------------

(defun poller-log-error (err)
  "Default logger for ERR."
  (message "[poller] error: %S" err))

;;;; -------------------------------------------------------------------
;;;; Registry
;;;; -------------------------------------------------------------------

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
     (equal (poller-job-name job)
            name))
   poller-jobs))

(defun poller-remove-job (name)
  "Remove poller job by NAME."

  (poller-stop-job name)

  (setq poller-jobs
        (cl-remove-if
         (lambda (job)
           (equal (poller-job-name job)
                  name))
         poller-jobs)))

(defun poller-enable-job (name)
  "Enable poller job NAME."

  (when-let ((job (poller-get-job name)))
    (setf (poller-job-enabled job) t)))

(defun poller-disable-job (name)
  "Disable poller job NAME."

  (when-let ((job (poller-get-job name)))
    (setf (poller-job-enabled job) nil)))

;;;; -------------------------------------------------------------------
;;;; Core Runner
;;;; -------------------------------------------------------------------

(defun poller-run-job (job)
  "Run JOB asynchronously."

  (when (poller-job-enabled job)

    ;; prevent overlapping runs
    (unless (poller-job-running job)

      (setf (poller-job-running job) t)
      (setf (poller-job-last-run job)
            (current-time))

      (run-hook-with-args
       'poller-before-run-hook
       job)

      (condition-case outer-err

          (funcall

           (poller-job-request job)

           ;; success callback
           (lambda (raw-data)

             (unwind-protect

                 (condition-case parse-err

                     (let* ((parser
                             (or (poller-job-parser job)
                                 #'identity))

                            (parsed
                             (funcall parser raw-data))

                            (target
                             (poller-job-target job)))

                       ;; runtime state
                       (setf (poller-job-last-success job)
                             (current-time))

                       (setf (poller-job-last-error job)
                             nil)

                       (setf (poller-job-last-result job)
                             parsed)

                       ;; target update
                       (when target
                         (funcall target parsed))

                       ;; success hooks
                       (run-hook-with-args
                        'poller-after-success-hook
                        job
                        parsed))

                   (error

                    (setf (poller-job-last-error job)
                          parse-err)

                    (run-hook-with-args
                     'poller-after-error-hook
                     job
                     parse-err)

                    (funcall
                     (or (poller-job-on-error job)
                         poller-default-error-handler)
                     parse-err)))

               ;; always cleanup
               (setf (poller-job-running job) nil)

               (run-hook-with-args
                'poller-after-finish-hook
                job)))

           ;; error callback
           (lambda (err)

             (unwind-protect

                 (progn

                   (setf (poller-job-last-error job)
                         err)

                   (run-hook-with-args
                    'poller-after-error-hook
                    job
                    err)

                   (funcall
                    (or (poller-job-on-error job)
                        poller-default-error-handler)
                    err))

               ;; always cleanup
               (setf (poller-job-running job) nil)

               (run-hook-with-args
                'poller-after-finish-hook
                job))))

        ;; outer setup error
        (error

         (setf (poller-job-running job) nil)

         (setf (poller-job-last-error job)
               outer-err)

         (run-hook-with-args
          'poller-after-error-hook
          job
          outer-err)

         (funcall
          (or (poller-job-on-error job)
              poller-default-error-handler)
          outer-err)

         (run-hook-with-args
          'poller-after-finish-hook
          job))))))

;;;; -------------------------------------------------------------------
;;;; Startup / Shutdown
;;;; -------------------------------------------------------------------

(defun poller-start-job (job)
  "Start JOB using its trigger."

  ;; prevent duplicate timers/hooks
  (when (poller-job-started job)
    (poller-stop-job
     (poller-job-name job)))

  (when-let ((trigger
              (poller-job-trigger job)))

    (funcall trigger job)

    (setf (poller-job-started job) t)))

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
         (symbol-name
          (poller-job-name j)))
       poller-jobs)))))

  (let ((job
         (poller-get-job name)))

    (unless job
      (error "No such job: %S" name))

    (poller-run-job job)))

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

  (let ((job
         (poller-get-job name)))

    (unless job
      (error "No such job: %S" name))

    (dolist (handle
             (poller-job-handles job))

      (cond

       ((timerp handle)
        (cancel-timer handle))

       ((consp handle)
        (remove-hook
         (car handle)
         (cdr handle)))))

    (setf (poller-job-handles job) nil)
    (setf (poller-job-running job) nil)
    (setf (poller-job-started job) nil)))

(defun poller-stop-all ()
  "Stop all poller jobs."

  (interactive)

  (dolist (job poller-jobs)
    (poller-stop-job
     (poller-job-name job))))

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

      (setf (poller-job-handles job)
            (cons timer
                  (poller-job-handles job)))

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

      (setf (poller-job-handles job)
            (cons timer
                  (poller-job-handles job)))

      timer)))

(defun poller-hook-trigger (hook)
  "Create trigger that runs on HOOK."

  (lambda (job)

    (let ((fn
           (lambda (&rest _)
             (poller-run-job job))))

      (add-hook hook fn)

      (setf (poller-job-handles job)
            (cons (cons hook fn)
                  (poller-job-handles job)))

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

      (let ((inhibit-read-only t))

        (erase-buffer)

        (insert
         (format "%s" value))))))

;;;; -------------------------------------------------------------------
;;;; Request Helpers
;;;; -------------------------------------------------------------------

(defun poller-url-request (url)
  "Create async URL request for URL."

  (lambda (success error)

    (url-retrieve

     url

     (lambda (_status)

       (let ((buf
              (current-buffer)))

         (condition-case err

             (with-current-buffer buf

               (goto-char (point-min))

               (re-search-forward "\n\n")

               (let ((body
                      (buffer-substring
                       (point)
                       (point-max))))

                 (kill-buffer buf)

                 (funcall success body)))

           (error

            (when (buffer-live-p buf)
              (kill-buffer buf))

            (funcall error err))))))))

(defun poller-process-request (command &optional args)
  "Create async process request from COMMAND and ARGS."

  (lambda (success error)

    (let ((buffer
           (generate-new-buffer
            (format "*poller-%s*" command))))

      (make-process
       :name (format "poller-%s" command)

       :buffer buffer

       :command
       (append (list command)
               args)

       :sentinel
       (lambda (proc _event)

         (when (memq (process-status proc)
                     '(exit signal))

           (unwind-protect

               (if (= 0
                      (process-exit-status proc))

                   (with-current-buffer
                       (process-buffer proc)

                     (funcall success
                              (buffer-string)))

                 (funcall
                  error
                  (format
                   "Process failed: %s"
                   (process-exit-status proc))))

             (when (buffer-live-p
                    (process-buffer proc))

               (kill-buffer
                (process-buffer proc))))))))))

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
;;;; Macro API
;;;; -------------------------------------------------------------------

(cl-defmacro poller-define
    (name
     &key
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

     :on-error
     ,(or on-error
          'poller-default-error-handler)

     :enabled ,(not disabled)

     :metadata ,metadata

     :handles nil

     :running nil

     :started nil)))

(provide 'poller)

;;; poller.el ends here

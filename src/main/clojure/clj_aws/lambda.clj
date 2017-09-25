(ns clj-aws.lambda)

(require '[clj-aws.s3 :as s3])
(require '[clj-common.jvm :as jvm])
(require '[clj-common.json :as json])

(defn create-classlooader [parent-loader s3-client code-bucket]
  (let [available-cljs (into
                         #{}
                         (filter
                           #(.endsWith %1 ".clj")
                           (s3/list-objects s3-client code-bucket)))]
    (proxy
      [java.lang.ClassLoader] []
      ;                       (loadClass
      ;                         [name]
      ;                         (println "loading class:" name)
      ;                         (.loadClass compiler-loader name))
      (loadClass
        ([name]
         (println "loading class:" name)
         (.loadClass parent-loader name))
        ([name resolve]
         (println "loading class with resolve:" name)
         ; note
         ; calling public method of provided classloader, failing if calling with resolve arg
         (.loadClass parent-loader name)))
      (getResource
        [name]
        (println "getting resource:" name)
        (if
          (contains? available-cljs name)
          (do
            (println "live resource found:" name)
            (new java.net.URL (str "http://" code-bucket name)))
          (.getResource parent-loader name)))
      (getResourceAsStream
        [name]
        (println "getting resource as stream:" name)
        (if-let [input-stream (s3/input-stream s3-client code-bucket name)]
          (do
            (println "loading from dynamic storage")
            input-stream)
          (.getResourceAsStream parent-loader name))))))

(defn simple-handler [context request]
  {
    :status "ok"})

; to test from local see com.mungolab.aws.lambda.TestLambda

(defn generic-handler [input-stream output-stream context]
  (println "Env variables")
  (doseq [[key value] (jvm/environment-variables)]
    (println "Env:" key "value:" value))

  (let [request (json/read-keyworded input-stream)
        fn-name (symbol (or
                          (jvm/environment-variable "fn")
                          (:fn request)))
        ns-name (symbol (or
                          (jvm/environment-variable "ns")
                          (:ns request)))]

        ;code-bucket (jvm/environment-variable "code_bucket")
;        compiler-loader (if
;                          (bound? clojure.lang.Compiler/LOADER)
;                          @clojure.lang.Compiler/LOADER
;                          (.getClassLoader com.mungolab.aws.lambda.GenericHandler))
;        compiler-loader (.getClassLoader com.mungolab.aws.lambda.GenericHandler)
;        s3-client (s3/create-client)
;        available-cljs (into
;                         #{}
;                         (filter
;                           #(.endsWith %1 ".clj")
;                           (s3/list-objects s3-client code-bucket)))]
;    (println "available live resources:")
;    (run! println available-cljs)

    ; in local: sun.misc.Launcher$AppClassLoader
    ; on aws: java.net.URLClassLoader
;    (println "Compiler loader: " (class compiler-loader))

;    (with-bindings {
;                     clojure.lang.Compiler/LOADER
;                     (create-classlooader s3-client code-bucket)}
      (require (symbol ns-name) :reload-all)
      (let [fn-to-exec (ns-resolve (symbol ns-name) (symbol fn-name))]
        (println "function resolved")
        (if-let [result (fn-to-exec
                          context
                          request)]
          (json/write-to-stream result output-stream)
          nil))))
;)

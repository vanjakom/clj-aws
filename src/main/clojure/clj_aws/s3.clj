(ns clj-aws.s3
  (:require
   [clj-common.logging :as logging]
   [amazonica.aws.s3 :as amazonica])
  (:import
    com.amazonaws.services.s3.AmazonS3ClientBuilder
    com.amazonaws.services.s3.AmazonS3Client
    com.amazonaws.services.s3.model.ListObjectsV2Request
    com.amazonaws.services.s3.model.ListObjectsV2Result
    com.amazonaws.services.s3.model.S3ObjectSummary))

; s3 path structure
; [bucket-name prefix1 prefix2 name]
; prefix separator / by default

(defn create-client
  ([]
   (com.amazonaws.services.s3.AmazonS3ClientBuilder/defaultClient))
  ([access-key secret-key]
   (new
     com.amazonaws.services.s3.AmazonS3Client
     (new com.amazonaws.auth.BasicAWSCredentials access-key secret-key))))

(def ^:dynamic *client* (create-client))
(def ^:dynamic *delimiter* "/")

(defmacro with-client [client & expr]
  `(binding [*client* ~client]
     ~@expr))

(defn list-buckets []
  (.listBuckets *client*))

(defn list [path]
  (let [[bucket-name & prefix] path
        final-prefix (if (some? prefix)
                       (str (clojure.string/join *delimiter* prefix) *delimiter*)
                       nil)]
    (loop [continuation-token nil results []]
      (let [request (->
                      (new ListObjectsV2Request)
                      (.withDelimiter *delimiter*)
                      (.withBucketName bucket-name)
                      (.withPrefix final-prefix)
                      (.withContinuationToken continuation-token))
            response (.listObjectsV2 *client* request)
            new-results (into
                          (into
                            results
                            (map
                              #(conj
                                 path
                                 (.substring
                                   %
                                   (if (some? final-prefix) (.length final-prefix) 0)
                                   (dec (.length %))))
                              (.getCommonPrefixes response)))
                          (map
                            #(conj
                               path
                               (.substring
                                 (.getKey %)
                                 (if (some? final-prefix) (.length final-prefix) 0)
                                 (.length (.getKey %))))
                            (.getObjectSummaries response)))]
        (if-let [continuation-token (.getNextContinuationToken response)]
          (recur
            continuation-token new-results)
          (sort new-results))))))

(defn input-stream
  [path]
  (let [[bucket-name & name-seq] path
        key (clojure.string/join *delimiter* name-seq)]
    (println key)
    (object->input-stream
      (get-object *client* bucket-name key))))


(defn get-object-metadata
  [path]
  (let [[bucket-name & name-seq] path
        key (clojure.string/join *delimiter* name-seq)]
    (amazonica/get-object-metadata :bucket-name bucket-name :key key)))

(defn object-metadata->on-s3
  "Checks if object is available on S3, in case it was on Glacier"
  [object-metadata]
  
  


(defn get-object
  ([bucket-name key]
   (get-object *client* bucket-name key))
  ([client bucket-name key]
   (.getObject client bucket-name key)))
   

(defn put-object
  ([bucket-name key input-stream]
   (put-object *client* bucket-name key))
  ([request]
   (put-object *client* request))
  ([client bucket-name key input-stream]
   (.putObject
     client
     bucket-name
     key
     input-stream
     (new com.amazonaws.services.s3.model.ObjectMetadata)))
  ([client request]
   (.putObject
     client
     request)))

(defn get-object-range [bucket-name key start offset]
  (let [request (new com.amazonaws.services.s3.model.GetObjectRequest bucket-name key)]
    (.setRange request start (+ start offset))
    (.getObject *client* request)))

(defn get-object-metadata [bucket-name key]
  (.getObjectMetadata *client* bucket-name key))

; transformers

(defn bucket->bucket-name [bucket]
  (.getName bucket))

(defn object->metadata [object]
  (.getObjectMetadata object))

(defn metadata->raw-metadata [object-metadata]
  (.getRawMetadata object-metadata))

(defn object->raw-metadata [object]
  (.getRawMetadata (.getObjectMetadata object)))

(defn object->input-stream [object]
  (.getObjectContent object))


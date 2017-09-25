(ns clj-aws.s3)

(def ^:dynamic *client* nil)

(defn create-client
  ([]
   (com.amazonaws.services.s3.AmazonS3ClientBuilder/defaultClient))
  ([access-key secret-key]
   (new
     com.amazonaws.services.s3.AmazonS3Client
     (new com.amazonaws.auth.BasicAWSCredentials access-key secret-key))))

(defmacro with-client [client & expr]
  `(binding [*client* ~client]
     ~@expr))

(defn list-buckets []
  (.listBuckets *client*))

(defn list-objects
  ([bucket-name]
   (list-objects *client* bucket-name))
  ([client bucket-name]
   (map
     #(.getKey %1)
     (.getObjectSummaries (.listObjects client bucket-name)))))

(defn get-object
  ([bucket-name key]
   (get-object *client* bucket-name key))
  ([client bucket-name key]
   (.getObject client bucket-name key)))

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

(defn input-stream
  ([bucket-name key]
   (input-stream *client* bucket-name key))
  ([client bucket-name key]
   (if-let [object (get-object client bucket-name key)]
     (object->input-stream object)
     nil)))

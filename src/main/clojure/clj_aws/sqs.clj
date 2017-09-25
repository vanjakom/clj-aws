(ns clj-aws.sqs)

(require '[clj-aws.regions :as regions])

(defn create-client [region credentials]
  (let [client (->
                 (com.amazonaws.services.sqs.AmazonSQSClientBuilder/standard)
                 (.withRegion (regions/resolve region))
                 (.withCredentials (new
                                    com.amazonaws.auth.AWSStaticCredentialsProvider
                                    (new
                                      com.amazonaws.auth.BasicAWSCredentials
                                      (:access-key credentials)
                                      (:secret-key credentials))))
                 (.build))]
    client))

(defn get-queue [client queue-name]
  {
    :client client
    :url (.getQueueUrl (.getQueueUrl client queue-name))})

(defn number-of-messages-available [queue]
  (.get
    (.getAttributes
      (.getQueueAttributes
        (:client queue)
        (.withAttributeNames

          (.withQueueUrl
            (new com.amazonaws.services.sqs.model.GetQueueAttributesRequest)
            (:url queue))
          ["All"])))
    "ApproximateNumberOfMessages"))

(defn send-message [queue message]
  ; unwrap response
  (.sendMessage
    (:client queue)
    (->
      (new com.amazonaws.services.sqs.model.SendMessageRequest)
      (.withQueueUrl (:url queue))
      (.withMessageBody message)))
  nil)

(defn recieve-message [queue]
  (let [response (.receiveMessage
                   (:client queue)
                   (->
                     (new com.amazonaws.services.sqs.model.ReceiveMessageRequest)
                     (.withQueueUrl (:url queue))))]
    (if-let [message (first (.getMessages response))]
      {
        :handle (.getReceiptHandle message)
        :body (.getBody message)}
      nil)))

(defn delete-message [queue message]
  (.deleteMessage
    (:client queue)
    (:url queue)
    (:handle message))
  nil)

(defn disconnect [client]
  (.shutdown client))

(defmacro with-queue [[queue [region credentials queue-name]] & body]
  `(let [~queue (get-queue (create-client ~region ~credentials) ~queue-name)]
     (try
      (do
        ~@body)
      (finally (disconnect (:client ~queue))))))

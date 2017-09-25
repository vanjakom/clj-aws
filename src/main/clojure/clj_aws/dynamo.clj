(ns clj-aws.dynamo)

(def ^:dynamic *client* nil)

(def endpoints {
                 :east "http://dynamodb.us-east-1.amazonaws.com"
                 :west "http://dynamodb.us-west-2.amazonaws.com"})

(declare map->item-map)
(declare item-map->map)

(defn create-client [access-key secret-key endpoint]
  (doto
    (new
      com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
      (new com.amazonaws.auth.BasicAWSCredentials access-key secret-key))
    (.setEndpoint (endpoints endpoint))))

(defmacro with-client [client & expr]
  `(binding [*client* ~client]
     ~@expr))

(defn get-object [table query]
  (let [item-map (map->item-map query)
        request (new
                  com.amazonaws.services.dynamodbv2.model.GetItemRequest
                  table
                  item-map)]
    (.getItem *client* request)))

(defn value->atrribute-value
  ([value]
   (cond
     (instance? String value) (.withS
                                (new com.amazonaws.services.dynamodbv2.model.AttributeValue)
                                value)
     (instance? Long value) (.withN
                              (new com.amazonaws.services.dynamodbv2.model.AttributeValue)
                              (str value))
     (instance? Integer value) (.withN
                                 (new com.amazonaws.services.dynamodbv2.model.AttributeValue)
                                 (str value))
     :else (throw (ex-info "unsupported value type" {:value value :type (class value)})))))

(defn attribute-value->value
  [value]
  (if-let [value-s (.getS value)]
    value-s
    (if-let [value-n (.getN value)]
      (Long/parseLong value-n))))

(defn map->item-map [object]
  (reduce
    (fn [item-map [key value]]
      (assoc
        item-map
        (name key)
        (value->atrribute-value value)))
    {}
    object))

(defn item-map->map [item-map]
  (reduce
    (fn [object [key value]]
      (assoc
        object
        (keyword key)
        (attribute-value->value value)))
    {}
    item-map))



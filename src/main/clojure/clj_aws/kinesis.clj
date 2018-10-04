(ns clj-aws.kinesis
  (:require
    [amazonica.aws.kinesis :as kinesis]
    [clojure.core.async :as async]
    [clj-common.logging :as logging]
    [clj-common.json :as json]
    [clj-common.io :as io]
    [clj-common.jvm :as jvm]
    clj-common.async))

(def ^:dynamic *channel-size* 1000)

(defn create-async-delivery
  "Creates async delivery to Kinesis stream. Returns fn with two arities, when
  called with arrity zero, delivery stream will be closed. Arity one will deliver
  message. Message should be map {:partition ^String :body ^Map}"
  [stream]
  (let [channel (async/chan *channel-size*)]
    (async/go
      (loop []
        (if-let [{partition :partition message :body} (async/<! channel)]
          (do
            (try
              (kinesis/put-record
                stream
                (io/string->byte-buffer
                  (str (json/write-to-string message) "\n"))
                partition)
              (catch Exception e (logging/report-throwable e)))
            (recur))
          (logging/report "channel closed, stopping"))))
    (fn
      ([] (async/close! channel))
      ([message] (async/>!! channel message)))))

(defn follow-stream [stream callback-fn]
  (let [[worker worker-id] (kinesis/worker
                             :app "test-worker"
                             :stream stream
                             :checkpoint false ;; default to disabled checkpointing, can still force
                             ;; a checkpoint by returning true from the processor function
                             :processor (fn [records]
                                          (doseq [row records]
                                            (callback-fn row))))]
    (future (.run worker))
    worker))

(defn stop-worker
  "To be called with value returned by follow-stream"
  [worker]
  (.shutdown worker))


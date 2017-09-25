(defproject com.mungolab/clj-aws "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "https://github.com/vanjakom/clj-aws"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :dependencies [
                  [org.clojure/clojure "1.8.0"]
                  [lein-light-nrepl "0.3.2"]

                  [com.amazonaws/aws-java-sdk-s3 "1.11.158"]
                  [com.amazonaws/aws-java-sdk-dynamodb "1.11.158"]
                  [com.amazonaws/aws-java-sdk-sqs "1.11.158"]
                  [com.amazonaws/aws-lambda-java-core "1.1.0"]

                  [com.mungolab/clj-common "0.1.0-SNAPSHOT"]]
  :repl-options {
                  :nrepl-middleware [lighttable.nrepl.handler/lighttable-ops]})

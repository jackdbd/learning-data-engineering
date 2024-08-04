(ns user
  (:require [clojure.java.io :as io]
            [next.jdbc :as jdbc]
            [nextjournal.clerk :as clerk]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.sql :as ds-sql]))

(def db-file (io/file ".." "data" "nyc_311.duckdb"))
(def db-filepath (.getCanonicalPath db-file))
(def table-name "nyc_311_data.raw_data")

;; https://cljdoc.org/d/seancorfield/next.jdbc/1.2.659/doc/getting-started
(def db {:dbtype "duckdb" :dbname db-filepath})
(def datasource (jdbc/get-datasource db))
(def conn (jdbc/get-connection datasource))

(def query (str "SELECT * FROM " table-name " LIMIT 3"))
  
(jdbc/execute! datasource [query])

(def num-records
  (let [xs (jdbc/execute! datasource [(str "SELECT count(*) AS num_records FROM " table-name)])]
    (get (first xs) :num_records)))

(def nyc-sample-records
  (let [xs (jdbc/execute! datasource [(str "SELECT * FROM " table-name " LIMIT 3")])]
    xs))

(defn foo [n]
  (+ 1 2 n))

;; start Clerk's built-in webserver and open the browser when ready
(clerk/serve! {:browse true :port 7777 :watch-paths ["notebooks" "src"]})

(comment
  (foo 3)
  (ds-sql/sql->dataset conn query)
  )
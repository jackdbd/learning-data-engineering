^{:nextjournal.clerk/visibility {:code :hide}}
(ns notebooks.nyc
 (:require [meta-csv.core :as csv]
           [nextjournal.clerk :as clerk]
           [user]))

;; Calling function `foo` of namespace `user` with argument `3`:

(user/foo 3)

;; Clerk provides a built-in data table viewer that supports the three
;; most common tabular data shapes out of the box: a sequence of maps,
;; where each map's keys are column names; a seq of seq, which is just
;; a grid of values with an optional header; a map of seqs, in with
;; keys are column names and rows are the values for that column.

(clerk/table
 (csv/read-csv "https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv"))

;; Clerk also has built-in support for Plotly's low-ceremony plotting:
(clerk/plotly
 {:data [{:z [[1 2 3] [3 2 1]] :type "surface"}]})

(str ";; The table " user/table-name " has " user/num-records " records.")

(clerk/table
 user/nyc-sample-records)

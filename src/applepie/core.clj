(ns applepie.core
  (:require [compojure.route :only [files not-found]]
            [org.httpkit.server :as kit]
            [compojure.handler :only [site]]
            [compojure.core :only [defroutes GET POST DELETE ANY context]]))


 (defn foo
   "I don't do a whole lot."
   [x]
   (println x "Hello, World!"))


 (defn app [req]
   {:status  200
    :headers {"Content-Type" "text/html"}
    :body    "hello HTTP!"})

 (kit/run-server app {:port 8080})

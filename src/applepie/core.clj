(ns applepie.core
  (:use [compojure.route :only [files not-found files]]
        [org.httpkit.server :only [run-server]]
        [compojure.handler :only [site]]
        [compojure.core :only [defroutes GET POST DELETE ANY context]]
        [ring.util.response :only [redirect file-response resource-response content-type]]))



(defroutes all-routes
           ;           (GET "/" [] (file-response "index.html" {:root "public"}))
           (GET "/" [] (content-type (resource-response "index.html" {:root "public"}) "text/html"))

           (files "/static/")                         ;; static file url prefix /static, in `public` folder
           (not-found "<p>Page not found.</p>"))      ;; all other, return 404

(defonce server (atom nil))

(defn stop-server []
  (when-not (nil? @server)
    ;; graceful shutdown: wait 100ms for existing requests to be finished
    ;; :timeout is optional, when no timeout, stop immediately
    (@server :timeout 100)
    (reset! server nil)))

(stop-server)
(reset! server (run-server (site #'all-routes) {:port 8080}))


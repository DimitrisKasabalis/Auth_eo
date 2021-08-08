param ([Switch]$push)

Invoke-Expression -Command "docker compose build worker"

if ($push) {
    Invoke-Expression -Command "docker push vesnikos/eo_engine"
}

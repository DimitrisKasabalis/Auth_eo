param ([Switch]$push)
$VERSION = '0.2.0'

Invoke-Expression -Command "docker compose build worker"
Invoke-Expression -Command "docker tag vesnikos/eo_engine:${VERSION} vesnikos/eo_engine:latest"

if ($push) {
    Invoke-Expression -Command "docker push vesnikos/eo_engine:latest"
    Invoke-Expression -Command "docker push vesnikos/eo_engine:${VERSION}"
}

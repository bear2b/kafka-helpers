# kafka-helpers
Package to simplify work with Kafka

### Import
```
import (
    kafka "github.com/bear2b/kafka-helpers"
)
```
go get github.com/bear2b/kafka-helpers

In case of problems with import:
git config --global --add url."git@github.com:".insteadOf "https://github.com/"
Source: https://jacopretorius.net/2018/05/git-error-could-not-read-username.html

Check current GOPRIVATE var:
```
go env
```
And add private repo to GOPRIVATE (separate by comma in case of many)
```
export GOPRIVATE="github.com/bear2b/kafka-helpers"
```
Source of info:
https://medium.com/mabar/today-i-learned-fix-go-get-private-repository-return-error-reading-sum-golang-org-lookup-93058a058dd8

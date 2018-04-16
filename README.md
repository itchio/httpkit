# httpkit

![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)
[![build status](https://git.itch.ovh/itchio/httpkit/badges/master/build.svg)](https://git.itch.ovh/itchio/httpkit/commits/master)
[![codecov](https://codecov.io/gh/itchio/httpkit/branch/master/graph/badge.svg)](https://codecov.io/gh/itchio/httpkit)
[![Go Report Card](https://goreportcard.com/badge/github.com/itchio/httpkit)](https://goreportcard.com/report/github.com/itchio/httpkit)

## timeout

Provide an `*http.Client` that times out if connection takes too long or
if the connection is idle for a while.

## retrycontext

Implements exponential backoff

## uploader

Implements resumable uploads to Google Cloud Storage

## htfs

Access an HTTP file as if it were local, with expiring URL support

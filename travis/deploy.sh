#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
  if [ ! -z "$TRAVIS_TAG" ]; then
    mvn clean deploy --settings travis/mvn-settings.xml -B -U -P travis,oss-release "$@"
  fi
fi

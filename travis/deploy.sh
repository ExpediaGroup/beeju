#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
  if [ ! -z "$TRAVIS_TAG" ]
  then
    mvn deploy -P sonatype-oss-release --settings travis/mvn-settings.xml
  fi
fi

#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
  mkdir -p $GPG_DIR
  echo $GPG_PUBLIC_KEYS | base64 --decode >> ${env.GPG_DIR}/pubring.gpg
  echo $GPG_SECRET_KEYS | base64 --decode >> ${env.GPG_DIR}/secring.gpg
fi

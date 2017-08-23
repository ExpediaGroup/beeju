#!/usr/bin/env bash

set -e

#if [ "$TRAVIS_PULL_REQUEST" == 'false' ] && [ ! -z "$TRAVIS_TAG" ]; then
if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
  echo "Installing keyrings"
  mkdir -p $GPG_DIR
  echo $GPG_PUBLIC_KEYS | base64 --decode >> ${GPG_DIR}/pubring.gpg
  echo $GPG_SECRET_KEYS | base64 --decode >> ${GPG_DIR}/secring.gpg
  gpg2 --no-default-keyring --keyring ${GPG_DIR}/pubring.gpg --list-keys
  gpg2 --no-default-keyring --keyring ${GPG_DIR}/secring.gpg --list-secret-keys
fi

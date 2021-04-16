#!/bin/sh

rm -rf "$HOME/.ivy2/local"                                                        || true
find $HOME/.ivy2/cache                       -name "ivydata-*.properties" -delete || true
find $HOME/.cache/coursier/v1                -name "ivydata-*.properties" -delete || true
find $HOME/.sbt                              -name "*.lock"               -delete || true

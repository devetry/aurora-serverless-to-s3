#!/bin/bash
set -euo pipefail

# with thanks to https://alestic.com/2016/11/aws-lambda-awscli/

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

tmpdir=$(mktemp -d /tmp/lambda-XXXXXX)
virtualenv=$tmpdir/virtual-env
zipfile=$tmpdir/lambda.zip

trap 'rm -rf -- "$tmpdir"' EXIT

(
  virtualenv $virtualenv
  source $virtualenv/bin/activate
  pip install awscli
)

rsync -va $virtualenv/bin/aws $SCRIPT_DIR/../assets/exporter/aws
perl -pi -e '$_ = "#!/usr/bin/python\n" if $. == 1' $SCRIPT_DIR/../assets/exporter/aws
(cd $tmpdir; zip -r9 $zipfile aws)
(cd $virtualenv/lib/python2.7/site-packages; zip -r9 $zipfile .)

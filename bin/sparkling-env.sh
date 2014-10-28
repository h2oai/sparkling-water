if [ -z $TOPDIR ]; then
  echo "Caller has to setup TOPDIR variable!"
  exit -1
fi

VERSION=$( cat $TOPDIR/gradle.properties | grep version | sed -e "s/.*=//" )

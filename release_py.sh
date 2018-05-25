
for i in `seq 8 9`
do
    echo "Cleaning"
    gw clean
    rm -rf private
    echo "Releasing for 2.1.$i"
    git checkout tags/RELEASE-2.1.$i
    mkdir -p $(pwd)/private/
    echo "Setting pyenv env property"
    export relname=$(cat gradle.properties | grep "h2oMajorName" | cut -f2 -d=)
    export relbuild=$(cat gradle.properties | grep "h2oBuild" | cut -f2 -d=)
    export relmajor=$(cat gradle.properties | grep "h2oMajorVersion" | cut -f2 -d=)
    echo "http://h2o-release.s3.amazonaws.com/h2o/rel-${relname}/${relbuild}/Python/h2o-${relmajor}.${relbuild}-py2.py3-none-any.whl"
    curl -s http://h2o-release.s3.amazonaws.com/h2o/rel-${relname}/${relbuild}/Python/h2o-${relmajor}.${relbuild}-py2.py3-none-any.whl > $(pwd)/private/h2o.whl
    export H2O_PYTHON_WHEEL=$(pwd)/private/h2o.whl
    gw build -x check
    cd py/build/pkg
    python setup.py bdist_wheel
    cd dist
    twine upload * -u h2o -p 42pySparkling
    cd ..
    cd ../../..
done


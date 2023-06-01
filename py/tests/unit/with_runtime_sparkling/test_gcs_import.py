import h2o

def testImportFromGCS(hc):
    some_public_gcp_file = "gs://gcp-public-data-landsat" \
           "/LC08/01/001/009/LC08_L1GT_001009_20210612_20210622_01_T2/LC08_L1GT_001009_20210612_20210622_01_T2_MTL.txt"
    frame = h2o.import_file(path=some_public_gcp_file)
    df = hc.asSparkFrame(frame)
    assert df.count() == 218

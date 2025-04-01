from sentinelsat import SentinelAPI, make_path_filter
from datetime import datetime
from datetime import timedelta
import rasterio.features
import os.path
import hashlib
import uuid
import numpy as np
import pandas as pd
import fnmatch
import zipfile
import json
from tqdm import tqdm
from .utils import *

logger = logging.getLogger('sentinelloader')


class Sentinel2Loader:

    def __init__(self, dataPath, user, password, apiUrl='https://catalogue.dataspace.copernicus.eu/', showProgressbars=True,
                 dateToleranceDays=6, cloudCoverage=(0, 80), deriveResolutions=True, cacheApiCalls=True,
                 cacheTilesData=True, loglevel=logging.DEBUG, nirBand='B08', ignoreMissing=False, mosaic=False):
        logging.basicConfig(level=loglevel)
        self.api = SentinelAPI(user, password, apiUrl, show_progressbars=showProgressbars)
        self.dataPath = dataPath
        self.user = user
        self.password = password
        self.dateToleranceDays = dateToleranceDays
        self.cloudCoverage = cloudCoverage
        self.deriveResolutions = deriveResolutions
        self.cacheApiCalls = cacheApiCalls
        self.cacheTilesData = cacheTilesData
        self.nirBand = nirBand
        self.ignoreMissing = ignoreMissing
        self.mosaic = mosaic

    def getProductBandTiles(self, geoPolygon, dateReference):
        """Downloads and returns file names with Sentinel2 tiles that best fit the polygon area at the desired date reference. It will perform up/downsampling if deriveResolutions is True and the desired resolution is not available for the required band."""
        logger.debug("Getting contents. date=%s", dateReference)

        # find tiles that intercepts geoPolygon within date-tolerance and date+dateTolerance
        dateTolerance = timedelta(days=self.dateToleranceDays)
        dateObj = datetime.now()
        if dateReference != 'now':
            dateObj = datetime.strptime(dateReference, '%Y-%m-%d')

        dateFrom = dateObj
        dateTo = dateObj + dateTolerance

        if self.mosaic:
            dateFrom = dateFrom - dateTolerance
            dateTo = dateTo + timedelta(days=3*30)

        dateL2A = datetime.strptime('2018-12-18', '%Y-%m-%d')
        productLevel = '2A'
        if dateObj < dateL2A:
            logger.debug(
                'Reference date %s before 2018-12-18. Will use Level1C tiles (no atmospheric correction)' % (dateObj))
            productLevel = '1C'

        logger.debug("Querying API for candidate tiles")

        bbox = rasterio.features.bounds(geoPolygon)
        geoPolygon = [(bbox[0], bbox[3]), (bbox[0], bbox[1]),
           (bbox[2], bbox[1]), (bbox[2], bbox[3])]

        area = Polygon(geoPolygon).wkt

        logger.debug("Querying remote API")
        productType = "S2MSI_L3__MCQ" if self.mosaic else 'S2MSI%s' % productLevel
        collection = "GLOBAL-MOSAICS" if self.mosaic else "Sentinel2"
        cloudCoverage = None if self.mosaic else self.cloudCoverage
        products = self.api.query(area,
                                  date=(dateFrom.strftime("%Y%m%d"), dateTo.strftime("%Y%m%d")),
                                  productType=productType, cloudCover=cloudCoverage, order_by="startDate",
                                  collection=collection)
        logger.debug("Found %d products", len(products))

        if len(products) == 0:
            raise Exception('Could not find any tiles for the specified parameters')

        return products

    def _tidyCollars(self, sourceGeoTiffs):
        with tqdm(desc="Tidying up image collars",
                  total=len(sourceGeoTiffs),
                  unit="products") as progress:
            for tile in sourceGeoTiffs:
                os.system("nearblack %s -near 50 -nb 4" % tile)
                os.system("nearblack %s -white -near 50 -nb 4" % tile)
                progress.update()

    def retile_products(self, products, overlap=32):
        logger.debug("Retiling products with overlap %d" % overlap)
        zipdir = f"{self.dataPath}/extracted"
        virts = []

        paths = []
        for id in products:
            product = products[id]
            paths.append(f"{self.dataPath}/products/{product['title']}")

        with tqdm(desc="Unzipping products",
                  total=len(paths),
                  unit="tile") as progress:
            for zipp in paths:
                ziptitle = zipp.split("/")[-1]
                if os.path.exists(f"{zipdir}/{ziptitle}"):
                    continue
                with zipfile.ZipFile(zipp, 'r') as zipref:
                    zipref.extractall(zipdir)
                os.remove(zipp)
                progress.update()

        with tqdm(desc="Reprojecting image tiles",
                  total=len(products),
                  unit="tile") as progress:
            for id in products:
                product = products[id]
                bandpaths = f"{zipdir}/{product['title']}"
                virts.append(f"{bandpaths}/tile.vrt")
                os.system(f"gdalbuildvrt -q -separate %s %s %s %s %s"
                          % (f"{bandpaths}/rgb.vrt", f"{bandpaths}/B02.tif", f"{bandpaths}/B03.tif",
                             f"{bandpaths}/B04.tif", f"{bandpaths}/B08.tif"))
                os.system("gdalwarp -q -t_srs EPSG:3857 %s %s" %
                          (f"{bandpaths}/rgb.vrt", f"{bandpaths}/tile.vrt"))
                progress.update()

        source_tiles = ' '.join(virts)
        tilesize = 10008 + overlap*2

        logger.info("Building image mosaic VRT")
        os.system("gdalbuildvrt mosaic.vrt %s" % source_tiles)
        #os.system("gdalwarp -te %s %s %s %s virt.vrt virt2.vrt" % (s1[0], s1[1], s2[0], s2[1]))

        logger.info("Retiling images with tilesize %s" % tilesize)
        os.mkdir(f"{self.dataPath}/retiled")
        os.system(
            'gdal_retile -ps %s %s -overlap %s -targetDir %s %s' % (
                tilesize, tilesize, overlap, f"{self.dataPath}/retiled", "mosaic.vrt"))

        directory = os.fsencode(f"{self.dataPath}/retiled")
        dirlist = os.listdir(directory)
        processeddir = f"{self.dataPath}/processed"
        os.mkdir(processeddir)

        processpaths = []

        with tqdm(desc="Final tile preprocessing",
                  total=len(dirlist),
                  unit="tile") as progress:
            for file in dirlist:
                filename = os.fsdecode(file)
                filename_full = f"{self.dataPath}/retiled/{filename}"
                file_hash = hashlib.md5(filename.encode()).hexdigest()
                os.system("gdal_translate -q -of COG %s %s" %
                          (filename_full, f"{processeddir}/{file_hash}.tif"))
                os.remove(filename_full)
                processpaths.append(f"{processeddir}/{file_hash}.tif")
                progress.update()

        return processpaths

    def cropRegion(self, geoPolygon, sourceGeoTiffs):
        """Returns an image file with contents from a bunch of GeoTiff files cropped to the specified geoPolygon.
           Pay attention to the fact that a new file is created at each request and you should delete it after using it"""
        logger.debug("Cropping polygon from %d files" % (len(sourceGeoTiffs)))
        sourceGeoTiffs.reverse() # place cloudiest images at bottom
        desiredRegion = geoPolygon

        source_tiles = ' '.join(sourceGeoTiffs)
        tmp_file = "%s/tmp/%s.tiff" % (self.dataPath, uuid.uuid4().hex)
        if not os.path.exists(os.path.dirname(tmp_file)):
            os.makedirs(os.path.dirname(tmp_file))

        # define output bounds in destination srs reference
        bounds = desiredRegion.bounds
        s1 = convertWGS84To3857(bounds[0], bounds[1])
        s2 = convertWGS84To3857(bounds[2], bounds[3])

        self._tidyCollars(sourceGeoTiffs)

        logger.debug('Combining tiles into a single image. sources=%s tmpfile=%s' % (source_tiles, tmp_file))
        os.system('gdalwarp  -co "TILED=YES" -co "COMPRESS=JPEG" -co "BIGTIFF=YES" -multi -srcnodata 0 -t_srs EPSG:3857 -te %s %s %s %s %s %s' % (
        s1[0], s1[1], s2[0], s2[1], source_tiles, tmp_file))

        return tmp_file

    def getRegionHistory(self, geoPolygon, bandOrIndexName, resolution, dateFrom, dateTo, daysStep=5,
                         ignoreMissing=True, interpolateMissingDates=False):
        """Gets a list of tiles covering entire polygon from date range with minimal clouds"""
        logger.info(
            "Getting region history for band %s from %s to %s at %s" % (bandOrIndexName, dateFrom, dateTo, resolution))
        dateFromObj = datetime.strptime(dateFrom, '%Y-%m-%d')
        dateToObj = datetime.strptime(dateTo, '%Y-%m-%d')
        dateRef = dateFromObj
        self.dateToleranceDays = daysStep

        lastSuccessfulFile = None
        pendingInterpolations = 0
        products = dict()

        area_hash = hashlib.md5(geoPolygon.wkt.encode()).hexdigest()
        apicache_file = self.dataPath + "/queries/Sentinel-2-S2MSI%s-%s-%s-%s-%s-%s.csv" % (
            '2A', area_hash, dateFromObj.strftime("%Y%m%d"), dateToObj.strftime("%Y%m%d"), self.cloudCoverage[0],
            self.cloudCoverage[1])
        should_fetch = True

        if self.cacheApiCalls and os.path.isfile(apicache_file):
            logger.debug("Using cached API query contents")
            products_df = pd.read_csv(apicache_file, index_col=0)
            os.system("touch -c %s" % apicache_file)
            should_fetch = False

        if should_fetch:
            while dateRef <= dateToObj:
                logger.debug(dateRef)
                dateRefStr = dateRef.strftime("%Y-%m-%d")
                regionFile = None
                try:

                    if bandOrIndexName in ['NDVI', 'NDWI', 'NDWI_MacFeeters', 'NDMI']:
                        regionFile = self.getRegionIndex(geoPolygon, bandOrIndexName, resolution, dateRefStr)
                    else:
                        products.update(self.getProductBandTiles(geoPolygon, dateRefStr))

                except Exception as e:
                    if ignoreMissing:
                        logger.info("Couldn't get data for %s using the specified filter. err=%s" % (dateRefStr, e))
                    else:
                        if interpolateMissingDates:
                            if lastSuccessfulFile != None:
                                pendingInterpolations = pendingInterpolations + 1
                        else:
                            raise e

                dateRef = dateRef + timedelta(days=daysStep)
            products_df = self.api.to_dataframe(products)
            products_df["startDate"] = pd.to_datetime(products_df["startDate"])

        if self.cacheApiCalls:
            logger.debug("Caching API query results for later usage")
            saveFile(apicache_file, products_df.to_csv(index=True))

        return self.parseSearchResults(geoPolygon, products_df)

    def parseSearchResults(self, geoPolygon, products_df):
        missing = geoPolygon
        selectedTiles = []

        products_df_sorted = products_df.sort_values(['cloudCover', 'startDate'], ascending=[True, False])
        with tqdm(desc="Mapping area coverage",
                  total=len(products_df_sorted),
                  unit="products",
                  postfix=f" - Missing area: {missing.area}") as progress:
            for index, pf in products_df_sorted.iterrows():
                footprint = gmlToPolygon(pf['gmlgeometry'])
                if missing.area > 0 and missing.intersects(footprint):
                    missing = (missing.symmetric_difference(footprint)).difference(footprint)
                    selectedTiles.append(index)
                progress.update()
                progress.set_postfix_str(f"- Missing area: {missing.area}")

        if missing.area > 0 and not self.ignoreMissing:
            print(missing.wkt)
            raise Exception('Could not find tiles for the whole selected area at date range')

        selectedProducts = products_df_sorted.loc[selectedTiles]
        logger.info("Total %s products, total size: %s", len(selectedProducts), self._readbleSize(selectedProducts["download_size"].sum()))
        return selectedTiles

    def downloadProduct(self, id, nodefilter=None):
        if nodefilter is not None:
            nodefilter = make_path_filter(nodefilter, exclude=False)
        return self.api.download(id, f"{self.dataPath}/products", nodefilter=nodefilter)

    def downloadAll(self, products, nodefilter=None, checksum=True):
        hash = hashlib.md5(json.dumps(products).encode()).hexdigest()
        if os.path.isfile(f"{self.dataPath}/cache/{hash}.json"):
            return json.load(open(f"{self.dataPath}/cache/{hash}.json"))
        if nodefilter is not None:
            nodefilter = make_path_filter(nodefilter, exclude=False)

        dl, _, _ = self.api.download_all(products, f"{self.dataPath}/products", nodefilter=nodefilter, checksum=checksum)
        saveFile(f"{self.dataPath}/cache/{hash}.json", json.dumps(dl, indent=4, sort_keys=True, default=str))
        return dl

    def getNodePaths(self, product_infos, filefilter):
        def node_filter(node):
            return fnmatch.fnmatch(node, filefilter)
        paths = []
        for id in product_infos:
            product_info = product_infos[id]
            product_nodes = product_info["nodes"]
            for node in product_nodes:
                if node_filter(node):
                    paths.append(str(product_nodes[node]["path"]))
        return paths

    def getRegionBand(self, geoPolygon, bandName, resolution, dateReference):
        regionTileFiles = self.getProductBandTiles(geoPolygon, dateReference)
        return self.cropRegion(geoPolygon, regionTileFiles)

    def _readbleSize(self, bytesize):
        suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        i = 0
        while bytesize >= 1024 and i < len(suffixes) - 1:
            bytesize /= 1024.
            i += 1
        f = ('%.2f' % bytesize).rstrip('0').rstrip('.')
        return '%s %s' % (f, suffixes[i])

    def _getBandDataFloat(self, geoPolygon, bandName, resolution, dateReference):
        bandFile = self.getRegionBand(geoPolygon, bandName, resolution, dateReference)

        gdalBand = gdal.Open(bandFile)
        geoTransform = gdalBand.GetGeoTransform()
        projection = gdalBand.GetProjection()

        data = gdalBand.ReadAsArray().astype(np.float)
        os.remove(bandFile)
        return data, geoTransform, projection

    def getAreaMosaics(self, geoPolygon, year, quarter):
        month =  self._getMonthFromQuarter(quarter)

        area_hash = hashlib.md5(geoPolygon.wkt.encode()).hexdigest()
        apicache_file = self.dataPath + "/queries/Sentinel-2-S2MSI%s-%s-%s-%s.csv" % (
            '3', area_hash, year, quarter)
        should_fetch = True
        dateRefStr = f"{year}-{month}-01"

        if self.cacheApiCalls and os.path.isfile(apicache_file):
            logger.debug("Using cached API query contents")
            products_df = pd.read_csv(apicache_file, index_col=0)
            os.system("touch -c %s" % apicache_file)
            should_fetch = False

        if should_fetch:
            products = self.getProductBandTiles(geoPolygon, dateRefStr)
            products_df = self.api.to_dataframe(products)
            products_df["startDate"] = pd.to_datetime(products_df["startDate"])

        if self.cacheApiCalls:
            logger.debug("Caching API query results for later usage")
            saveFile(apicache_file, products_df.to_csv(index=True))

        return self.parseSearchResults(geoPolygon, products_df)

    def _getMonthFromQuarter(self, quarter):
        month = ((int(quarter[1]) - 1) * 3) + 1
        if len(str(month)) == 1:
            month = f"0{month}"
        return month

    def getRegionIndex(self, geoPolygon, indexName, resolution, dateReference):
        if indexName == 'NDVI':
            # get band 04
            red, geoTransform, projection = self._getBandDataFloat(geoPolygon, 'B04', resolution, dateReference)
            # get band 08
            nir, _, _ = self._getBandDataFloat(geoPolygon, self.nirBand, resolution, dateReference)
            # calculate ndvi
            ndvi = ((nir - red) / (nir + red))
            # save file
            tmp_file = "%s/tmp/ndvi-%s.tiff" % (self.dataPath, uuid.uuid4().hex)
            saveGeoTiff(ndvi, tmp_file, geoTransform, projection)
            return tmp_file

        elif indexName == 'NDWI':
            # get band 08
            b08, geoTransform, projection = self._getBandDataFloat(geoPolygon, self.nirBand, resolution, dateReference)
            # get band 11
            b11, _, _ = self._getBandDataFloat(geoPolygon, 'B11', resolution, dateReference)
            # calculate
            ndwi = ((b08 - b11) / (b08 + b11))
            # save file
            tmp_file = "%s/tmp/ndwi-%s.tiff" % (self.dataPath, uuid.uuid4().hex)
            saveGeoTiff(ndwi, tmp_file, geoTransform, projection)
            return tmp_file

        elif indexName == 'NDWI_MacFeeters':
            # get band 03
            b03, geoTransform, projection = self._getBandDataFloat(geoPolygon, 'B03', resolution, dateReference)
            # get band 08
            b08, _, _ = self._getBandDataFloat(geoPolygon, self.nirBand, resolution, dateReference)
            # calculate
            ndwi = ((b03 - b08) / (b03 + b08))
            # save file
            tmp_file = "%s/tmp/ndwi-%s.tiff" % (self.dataPath, uuid.uuid4().hex)
            saveGeoTiff(ndwi, tmp_file, geoTransform, projection)
            return tmp_file

        elif indexName == 'NDMI':
            # get band 03
            nir, geoTransform, projection = self._getBandDataFloat(geoPolygon, 'B03', resolution, dateReference)
            # get band 08
            swir, _, _ = self._getBandDataFloat(geoPolygon, 'B10', resolution, dateReference)
            # calculate
            ndmi = ((nir - swir) / (nir + swir))
            # save file
            tmp_file = "%s/tmp/ndmi-%s.tiff" % (self.dataPath, uuid.uuid4().hex)
            saveGeoTiff(ndmi, tmp_file, geoTransform, projection)
            return tmp_file

        elif indexName == 'EVI':
            # https://github.com/sentinel-hub/custom-scripts/tree/master/sentinel-2
            # index = 2.5 * (B08 - B04) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)

            # get band 04
            b04, geoTransform, projection = self._getBandDataFloat(geoPolygon, 'B04', resolution, dateReference)
            # get band 08
            b08, _, _ = self._getBandDataFloat(geoPolygon, self.nirBand, resolution, dateReference)
            # get band 02
            b02, _, _ = self._getBandDataFloat(geoPolygon, 'B02', resolution, dateReference)
            # calculate
            evi = 2.5 * (b08 - b04) / ((b08 + (6.0 * b04) - (7.5 * b02)) + 1.0)
            # save file
            tmp_file = "%s/tmp/ndmi-%s.tiff" % (self.dataPath, uuid.uuid4().hex)
            saveGeoTiff(evi, tmp_file, geoTransform, projection)
            return tmp_file

        else:
            raise Exception('\'indexName\' must be NDVI, NDWI, NDWI_MacFeeters, or NDMI')

    def cleanupCache(self, filesNotUsedDays):
        os.system("find %s -type f -name '*' -mtime +%s -exec rm {} \;" % (self.dataPath, filesNotUsedDays))

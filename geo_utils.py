"""
Geospatial Utilities

Many assume the standard North up, which is not always true
"""
import math
from collections import namedtuple

from osgeo import gdal, ogr


GeoExtent = namedtuple('GeoExtent', ['x_min', 'y_max', 'x_max', 'y_min'])
GeoAffine = namedtuple('GeoAffine', ['ul_x', 'x_res', 'rot_1', 'ul_y', 'rot_2', 'y_res'])
GeoCoordinate = namedtuple('GeoCoordinate', ['x', 'y'])
RowColumn = namedtuple('RowColumn', ['row', 'column'])
RowColumnExtent = namedtuple('RowColumnExtent', ['start_row', 'start_col', 'end_row', 'end_col'])

CONUS_EXTENT = GeoExtent(x_min=-2565585,
                         y_min=14805,
                         x_max=2384415,
                         y_max=3314805)


def shapefile_extent(shapefile):
    ds = ogr.Open(shapefile)
    layer = ds.GetLayer()
    ext1 = layer.GetExtent()

    return GeoExtent(x_min=ext1[0],
                     x_max=ext1[1],
                     y_min=ext1[2],
                     y_max=ext1[3])


def epsg_from_shapefile(shapefile):
    ds = ogr.Open(shapefile)
    layer = ds.GetLayer()
    spatialref = layer.GetSpatialRef()

    return spatialref.ExportToEPSG()


def fifteen_offset(coord):
    return (coord // 30) * 30 + 15


def geo_to_rowcol(affine, coord):
    """
    Yline = (Ygeo - GT(3) - Xpixel*GT(4)) / GT(5)
    Xpixel = (Xgeo - GT(0) - Yline*GT(2)) / GT(1)

    :param affine:
    :param coord:
    :return:
    """
    # floor and ceil probably depends on rotation, but use standard for N up
    col = math.floor((coord.x - affine.ul_x - affine.ul_y * affine.rot_1) / affine.x_res)
    row = math.ceil((coord.y - affine.ul_y - affine.ul_x * affine.rot_2) / affine.y_res)

    return RowColumn(row=int(row),
                     column=int(col))


def rowcol_to_geo(affine, rowcol):
    """
    Xgeo = GT(0) + Xpixel*GT(1) + Yline*GT(2)
    Ygeo = GT(3) + Xpixel*GT(4) + Yline*GT(5)

    :param affine:
    :param rowcol:
    :return:
    """
    x = affine.ul_x + rowcol.column * affine.x_res + rowcol.row * affine.rot_1
    y = affine.ul_y + rowcol.column * affine.rot_2 + rowcol.row * affine.y_res

    return GeoCoordinate(x=x, y=y)


def rowcolext_to_components(rowcol_ext):
    """
    Split the extent into it's components

    :param rowcol_ext:
    :return:
    """
    ul = RowColumn(row=rowcol_ext.start_row, column=rowcol_ext.start_col)
    lr = RowColumn(row=rowcol_ext.end_row, column=rowcol_ext.end_col)

    return ul, lr


def geoext_to_components(geo_ext):
    """
    Split the extent into it's components

    :param geo_ext:
    :return:
    """
    ul = GeoCoordinate(x=geo_ext.x_min, y=geo_ext.y_max)
    lr = GeoCoordinate(x=geo_ext.x_max, y=geo_ext.y_min)

    return ul, lr


def rowcolext_to_geoext(affine, rowcol_ext):
    """
    Convert extent from row/col to a spatial extent

    :param affine:
    :param rowcol_ext:
    :return:
    """
    ul, lr = rowcolext_to_components(rowcol_ext)

    geo_ul = rowcol_to_geo(affine, ul)
    geo_lr = rowcol_to_geo(affine, lr)

    return GeoExtent(x_min=geo_ul.x,
                     x_max=geo_lr.x,
                     y_min=geo_lr.y,
                     y_max=geo_ul.y)


def geoext_to_rowcolext(geo_extent, affine):
    """
    Convert a spatial extent to a row/col extent

    :param geo_extent:
    :param affine:
    :return:
    """
    ul, lr = geoext_to_components(geo_extent)

    rc_ul = geo_to_rowcol(affine, ul)
    rc_lr = geo_to_rowcol(affine, lr)

    return RowColumnExtent(start_row=rc_ul.row,
                           end_row=rc_lr.row,
                           start_col=rc_ul.column,
                           end_col=rc_lr.column)


def get_raster_ds(raster_file, readonly=True):
    if readonly:
        return gdal.Open(raster_file, gdal.GA_ReadOnly)
    else:
        return gdal.Open(raster_file, gdal.GA_Update)


def get_raster_geoextent(raster_file):
    ds = get_raster_ds(raster_file)

    affine = get_raster_affine(raster_file)
    rowcol = RowColumn(row=ds.RasterYSize, column=ds.RasterXSize)

    geo_lr = rowcol_to_geo(affine, rowcol)

    return GeoExtent(x_min=affine.ul_x, x_max=geo_lr.x,
                     y_min=geo_lr.y, y_max=affine.ul_y)


def get_raster_affine(raster_file):
    """
    Retrieve the affine/Geo Transform from a raster

    :param raster_file:
    :return:
    """
    ds = get_raster_ds(raster_file)

    return GeoAffine(*ds.GetGeoTransform())


def array_from_rasterband(raster_file, geo_extent=None, band=1):
    ds = get_raster_ds(raster_file)

    if geo_extent:
        affine = get_raster_affine(raster_file)

        ul_geo = GeoCoordinate(x=geo_extent.x_min, y=geo_extent.y_max)
        lr_geo = GeoCoordinate(x=geo_extent.x_max, y=geo_extent.y_min)

        ul_rc = geo_to_rowcol(affine, ul_geo)
        lr_rc = geo_to_rowcol(affine, lr_geo)

        return ds.GetRasterBand(band).ReadAsArray(ul_rc.column,
                                                  ul_rc.row,
                                                  lr_rc.column - ul_rc.column,
                                                  lr_rc.row - ul_rc.row)

    else:
        return ds.GetRasterBand(band).ReadAsArray()


def extent_from_hv(h, v, loc='conus'):
    """
    Retrieve the geospatial extent for the given tile h/v

    :param h:
    :param v:
    :param loc:
    :return:
    """
    loc = loc.lower()

    if loc == 'conus':
        xmin = CONUS_EXTENT.x_min + h * 5000 * 30
        xmax = CONUS_EXTENT.x_min + h * 5000 * 30 + 5000 * 30
        ymax = CONUS_EXTENT.y_max - v * 5000 * 30
        ymin = CONUS_EXTENT.y_max - v * 5000 * 30 - 5000 * 30
    else:
        raise Exception('Location not implemented: {0}'
                        .format(loc))

    return (GeoExtent(x_min=xmin, x_max=xmax, y_max=ymax, y_min=ymin),
            GeoAffine(ul_x=xmin, x_res=30, rot_1=0, ul_y=ymax, rot_2=0, y_res=30))

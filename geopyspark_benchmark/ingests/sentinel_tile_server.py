import io
import numpy as np
import rasterio

from flask import Flask, make_response
from PIL import Image
from PIL import ImageOps
from geopyspark.geopycontext import GeoPyContext
from geopyspark.geotrellis.catalog import read_value
from geopyspark.geotrellis.constants import SPATIAL


def make_image(arr):
    adjusted = ((arr - arr.min())/(arr.max() - arr.min()))*255
    return Image.fromarray(adjusted.astype('uint8')).resize((256,256), Image.NEAREST).convert('L')

    #return ImageOps.equalize(img, mask=mask)
    #return ImageOps.autocontrast(img, cutoff=1.5, ignore=0)


'''
def adjust_image(image):
    img = image.convert('L')
    arr = np.array(img)

    oldshape = arr.shape
    arr = arr.ravel()
    temp = template.ravel()

    a_values, bin_idx, a_counts = np.unique(arr, return_inverse=True, return_counts=True)
    t_values, t_counts = np.unque(temp, return_counts=True)

    a_quantiles = np.cumsum(a_counts).astype(np.float64)
    a_quantiles /= a_quantiles[-1]
    t_quantiles = np.cumsum(t_counts).astype(np.float64)
    t_quantiles /= t_quantiles[-1]

    interp_t_values = np.interp(a_quantiles, t_quantiles, t_values)

    return Image.fromarray(interp_t_values[bin_idx].reshape(oldshape)).convert('RGB')
'''


app = Flask(__name__)

@app.route("/<int:zoom>/<int:x>/<int:y>.png")
def tile(x, y, zoom):
    # fetch tile

    tile = read_value(geopysc, SPATIAL, uri, layer_name, zoom, x, y)
    arr = tile['data']

    bands = arr.shape[0]
    arrs = [np.array(arr[x, :, :]).reshape(256, 256) for x in range(bands)]

    # display tile
    images = [make_image(arr) for arr in arrs]
    image = ImageOps.autocontrast(Image.merge('RGB', images))
    #new_image = adjust_image(image)

    bio = io.BytesIO()
    image.save(bio, 'PNG')
    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    response.headers['Content-Disposition'] = 'filename=%d.png' % 0

    return response


if __name__ == "__main__":
    uri = "file:///tmp/sentinel-catalog"
    layer_name = "python-benchmark"

    geopysc = GeoPyContext(appName="s3-flask", master="local[*]")

    '''
    with rasterio.open('../../Desktop/18TVK-2016-10-18-0-60m.tif') as f:
        whole_tile = f.read()
    whole_image = Image.fromarray(whole_tile).convert('L')
    template = np.array(whole_image)
    '''

    app.run()

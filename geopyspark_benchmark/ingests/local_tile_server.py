import io
import numpy as np

from PIL import Image
from flask import Flask, make_response
from geopyspark.geopycontext import GeoPyContext
from geopyspark.geotrellis.catalog import read_value
from geopyspark.geotrellis.constants import SPATIAL


app = Flask(__name__)

@app.route("/<int:zoom>/<int:x>/<int:y>.png")
def tile(x, y, zoom):
    # fetch tile

    tile = read_value(geopycontext,
                      SPATIAL,
                      uri,
                      layer_name,
                      zoom,
                      x,
                      y)

    data = np.int32(tile['data']).reshape(256, 256)

    # display tile
    bio = io.BytesIO()
    im = Image.fromarray(data).resize((256, 256), Image.NEAREST).convert('L')
    im.save(bio, 'PNG')

    response = make_response(bio.getvalue())
    response.headers['Content-Type'] = 'image/png'
    response.headers['Content-Disposition'] = 'filename=%d.png' % 0

    return response


if __name__ == "__main__":
    uri = "file:///tmp/python-catalog/"
    layer_name = "python-benchmark"

    geopycontext = GeoPyContext(appName="s3-flask", master="local[*]")

    app.run()

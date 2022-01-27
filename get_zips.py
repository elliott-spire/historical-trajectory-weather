import requests
import zipfile
import io

RESPONSES = [
    {
        "creation_time": "2022-01-26T13:46:21.033090",
        "job_uuid": "randomstringuuid1",
    },
    {
        "creation_time": "2022-01-26T13:46:22.143236",
        "job_uuid": "randomstringuuid2",
    },
    {
        "creation_time": "2022-01-26T15:10:13.699509",
        "job_uuid": "randomstringuuid3",
    },
]

for x in RESPONSES:
    uuid = x["job_uuid"]
    zip_file_url = "https://api.wx.spire.com/export/{}/{}.zip".format(uuid, uuid)
    r = requests.get(zip_file_url, stream=True)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    for name in z.namelist():
        print(name)
        z.extract(name, path="ZIPS/")

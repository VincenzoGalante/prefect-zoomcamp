from prefect.filesystems import GitHub

block = GitHub(repository="https://github.com/VincenzoGalante/prefect-zoomcamp")
block.save("dtc-prefect-gh", overwrite=True)

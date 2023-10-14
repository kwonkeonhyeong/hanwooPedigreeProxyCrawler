def fileUpload(inputName):

    import pandas as pd
    
    import numpy as np

    fileData = pd.read_excel(f'{inputName}.xlsx',dtype='object')

    dataList = fileData.values.tolist()

    dataList = np.concatenate(dataList).tolist()

    return dataList

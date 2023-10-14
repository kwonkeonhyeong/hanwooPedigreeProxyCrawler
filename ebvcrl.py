from time import time
from weakref import proxy


def dataCrl(target,resultDict,keyDict):

    import requests
    from requests.sessions import Session
    from requests.adapters import HTTPAdapter, Retry
    from bs4 import BeautifulSoup as bs
    import xmltodict
    import json
    from collections import defaultdict
    import time

    def oneStopMakeUrl(target):
        url = f'https://chuksaro.nias.go.kr/hanwoori/cowCntcInfo.do?search_keyword={target}'
        return url


    def requestGet(targetUrl,proxyUse,protocol):
        
        with Session() as session:
            connect = 3
            read = 2
            backoff_factor = 0.2
            RETRY_AFTER_STATUS_CODES = (400, 401,403,404,408,500,502,503,504,522)

            retry = Retry(
                total = (connect + read),
                connect=connect,
                read=read,
                backoff_factor=backoff_factor,
                status_forcelist=RETRY_AFTER_STATUS_CODES,
            )

            adapter = HTTPAdapter(max_retries=retry)

            if protocol == 'https':
                session.mount("https://", adapter)

            if protocol == 'http':
                session.mount("http://", adapter)

            if proxyUse == 0:
                proxyUserName = 'farmplace'
                proxyPassWord = 'VKAvmf123'
                proxy = f'http://{proxyUserName}:{proxyPassWord}@gate.dc.smartproxy.com:20000'
                try:
                    response = session.get(url=targetUrl, proxies={'http': proxy, 'https': proxy},verify=False)
                except Exception as er:
                    time.sleep(4)
                    print(er)
                    return requestGet(targetUrl,proxyUse,protocol)

            if proxyUse == 1:
                try:
                    response = session.get(url=targetUrl,verify=False,timeout=3)
                except Exception as er:
                    time.sleep(4)
                    print(er)
                    return requestGet(targetUrl,proxyUse,protocol)

        return response
        
    def oneStopParser(target):
        oneStopUrl = oneStopMakeUrl(target)
        response = requestGet(oneStopUrl,0,"https")
        soup = bs(response.text,"html.parser")
        return soup


    def idvInfoValue(bs4soup,trIndex,tdIndex):
        idvInfo = bs4soup.find('table',attrs={'class':'table basic basic_info'}).find_all('tr')
        idvNumberHtml = idvInfo[trIndex].find_all('td')
        idvNumber = idvNumberHtml[tdIndex].string
        idvNumber = idvNumber.strip()
        return idvNumber
        
    def extractTargetTable(bs4soup,liValue,tableIndex):
        targetHtml = bs4soup.find('ul',attrs={'id':'tab_cont'}).find('li',attrs={'value':liValue}).find_all('table',attrs={'class':'table basic'})
        targetTable = targetHtml[tableIndex].find_all('tr')
        return targetTable

    def extractTargetValue(bs4soup,trIndex,tdIndex):
        targetRow = bs4soup[trIndex].find_all('td')
        targetValue = targetRow[tdIndex].string
        return targetValue


    def makeEbv(idv,cwtGrade,cwtEbv,emaGrade,emaEbv,btGrade,btEbv,msGrade,msEbv):
        ebv = []

        cwtGrade = str(cwtGrade)
        cwtGradeLen = len(cwtGrade)
        
        cwtEbv = str(cwtEbv)
        cwtEbvLen = len(cwtEbv)

        emaGrade = str(emaGrade)
        emaEbv = str(emaEbv)
        btGrade = str(btGrade)
        btEbv = str(btEbv)
        msGrade = str(msGrade)
        msEbv = str(msEbv)
        ebv.append(idv.replace(" ",""))
        
        ebv.append(cwtGrade[:cwtGradeLen-3])
        ebv.append(cwtEbv[:cwtEbvLen-1])
        ebv.append(emaGrade)
        ebv.append(emaEbv)
        ebv.append(btGrade)
        ebv.append(btEbv)
        ebv.append(msGrade)
        ebv.append(msEbv)

        return ebv


    oneStopSoup = oneStopParser(target)

    idvNumber = idvInfoValue(oneStopSoup,0,0)

    dataCheck = len(idvNumber)

    if dataCheck != 0:

        ebvInfo = extractTargetTable(oneStopSoup,'contact01',1)
        cwtGrade = extractTargetValue(ebvInfo,1,1)
        cwtEbv = extractTargetValue(ebvInfo,2,0)
        emaGrade = extractTargetValue(ebvInfo,1,2)
        emaEbv = extractTargetValue(ebvInfo,2,1)
        btGrade = extractTargetValue(ebvInfo,1,3)
        btEbv = extractTargetValue(ebvInfo,2,2)
        msGrade = extractTargetValue(ebvInfo,1,4)
        msEbv = extractTargetValue(ebvInfo,2,3)


        idvPedi = makeEbv(target,cwtGrade,cwtEbv,emaGrade,emaEbv,btGrade,btEbv,msGrade,msEbv)

        try:
            resultDict[target] = idvPedi
            print(idvPedi)
        except AttributeError:
            error = 'error'
            print('개체에러')
            if error in resultDict:
                resultDict[error] += [idvPedi]
            else:
                resultDict[error] = list()
                resultDict[error] += [idvPedi]
                pass
            

if __name__ == "__main__":
        
    from file import fileUpload
    import multiprocessing
    import pandas as pd
    import time
    start = time.time()  # 시작 시간 저장

    cowList = fileUpload('개체번호')

    m = multiprocessing.Manager()
                
    CrlResult = m.dict()

    keyDict = m.dict()
    keyDict['keyIndex'] = 0

    pool = multiprocessing.Pool(processes=4)

    pool.starmap(dataCrl,[(cow,CrlResult,keyDict) for cow in cowList])

    pool.close()
            
    pool.join()

    error = 'error'
    if error in CrlResult:
        idvErrorList = CrlResult.get(error)
        del(CrlResult[error])
    else:
        pass

    idvResultList = list(CrlResult.values())

    resultDfHeader = ["IID","cwtGrade","cwtEbv","emaGrade","emaEbv","bftGrade","btEbv","msGrade","msEbv"]
    crlResultDf = pd.DataFrame(idvResultList, columns = resultDfHeader, dtype= 'string')
    crlResultDf = crlResultDf.drop_duplicates(['IID'], keep='first')

    if 'idvErrorList' in vars():
        idvErrorDf = pd.DataFrame(idvErrorList, columns = resultDfHeader, dtype= 'string')

    with pd.ExcelWriter(f'EBV결과.xlsx') as writer:

            crlResultDf.to_excel(writer, sheet_name="EBV결과", index=False)  

            if 'idvErrorDf' in vars():
                idvErrorDf.to_excel(writer, sheet_name="개체크롤링오류", index=False)

        
    print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간


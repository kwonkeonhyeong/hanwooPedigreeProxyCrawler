from time import time
from weakref import proxy


def dataCrl(target, resultDict, keyDict):

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

    def openApiMakeUrl(target, keyDict):
        keyValue = keyDict['keyIndex']
        keyIndex = keyValue % 3
        key = [
            "Y6k9yR%2FboInZk%2BHcmOIBnrg5cfuVPmdp%2BXeHidOJW4Mgd2sdwszgUoZdumUheNqTAdlcWqdzcEhGztt3p3pBjA%3D%3D",
            "BO%2F3dIgEqyDE92lw4Uh7RJ7PudNPzn6TYr6L5dn3B98nhdBTovB4XiK4v8wjMfVf2B2zJjUQaBaC2rNC%2FQseAw%3D%3D",
            "TJJYF8erF9T6iz%2FqFHneEoTd8vIroczC1YXyUxQ5zVwl%2BLuoRDuLo5Muo0gdq94R%2FJH%2FQoLFf2%2FmjkEK%2BLb7Yg%3D%3D"
        ]
        option = 1
        url = f'http://data.ekape.or.kr/openapi-data/service/user/animalTrace/traceNoSearch?ServiceKey={key[keyIndex]}&traceNo={target}&optionNo={option}'
        keyValue += 1
        keyDict['keyIndex'] = keyValue
        return url

    def requestGet(targetUrl, proxyUse, protocol):

        with Session() as session:
            connect = 3
            read = 2
            backoff_factor = 0.2
            RETRY_AFTER_STATUS_CODES = (
                400, 401, 403, 404, 408, 500, 502, 503, 504, 522)

            retry = Retry(
                total=(connect + read),
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
                    response = session.get(url=targetUrl, proxies={
                                           'http': proxy, 'https': proxy}, verify=False)
                except Exception as er:
                    time.sleep(4)
                    print(er)
                    return requestGet(targetUrl, proxyUse, protocol)

            if proxyUse == 1:
                try:
                    response = session.get(
                        url=targetUrl, verify=False, timeout=3)
                except Exception as er:
                    time.sleep(4)
                    print(er)
                    return requestGet(targetUrl, proxyUse, protocol)

        return response

    def oneStopParser(target):
        oneStopUrl = oneStopMakeUrl(target)
        response = requestGet(oneStopUrl, 0, "https")
        soup = bs(response.text, "html.parser")
        print(soup)
        return soup

    def openApiParser(target, keyDict):
        openApiUrl = openApiMakeUrl(target, keyDict)
        response = requestGet(openApiUrl, 1, "http")
        xpars = xmltodict.parse(response.text)
        jsonDump = json.dumps(xpars)
        json_data = json.loads(jsonDump)
        return json_data

    def idvInfoValue(bs4soup, trIndex, tdIndex):
        idvInfo = bs4soup.find(
            'table', attrs={'class': 'table basic basic_info'}).find_all('tr')
        idvNumberHtml = idvInfo[trIndex].find_all('td')
        idvNumber = idvNumberHtml[tdIndex].string
        idvNumber = idvNumber.strip()
        return idvNumber

    def extractTargetTable(bs4soup, liValue, tableIndex):
        targetHtml = bs4soup.find('ul', attrs={'id': 'tab_cont'}).find(
            'li', attrs={'value': liValue}).find_all('table', attrs={'class': 'table basic'})
        targetTable = targetHtml[tableIndex].find_all('tr')
        return targetTable

    def extractTargetValue(bs4soup, trIndex, tdIndex):
        targetRow = bs4soup[trIndex].find_all('td')
        targetValue = targetRow[tdIndex].string
        return targetValue

    def ExtractValueOpenApi(jsonData, targetColumn):

        try:
            checkItemsType = jsonData['response']['body']['items']

            if not checkItemsType:
                resultValue = 'None'
                return resultValue
            else:
                checkItemType = checkItemsType['item']

                if str(type(checkItemType)) == "<class 'dict'>":
                    if targetColumn in dict.keys(checkItemType):
                        resultValue = checkItemType[targetColumn]
                        return resultValue
                    else:
                        resultValue = 'None'
                        return resultValue

                elif str(type(checkItemType)) == "<class 'list'>":

                    checkItemEleType = checkItemsType['item'][0]

                    if targetColumn in dict.keys(checkItemType):
                        resultValue = checkItemEleType[targetColumn]
                        return resultValue
                    else:
                        resultValue = 'None'
                        return resultValue
        except Exception as er:
            print(er)
            resultValue = 'None'
            return resultValue

    def overlapCheck(resultDict, target, overLapValue):
        if target in resultDict:
            originValue = resultDict.get(target)
            print("중복데이터")
            print(originValue)
            print(overLapValue)
            originNoneCount = originValue.count('None')
            print(originNoneCount)
            overLapNoneCount = overLapValue.count('None')
            print(overLapNoneCount)

            if originNoneCount < overLapNoneCount:
                resultDict[target] = originValue
                print("오리지날선택")
            elif originNoneCount > overLapNoneCount:
                resultDict[target] = overLapValue
                print("중복선택")
            elif originNoneCount == overLapNoneCount:
                idvDataCheck = overLapValue[2].replace(" ", "")
                originDataCheck = originValue[2].replace(" ", "")
                ("에러데이터발생")
                if idvDataCheck != originDataCheck:
                    error = 'error'
                    if error in resultDict:
                        resultDict[error] += [overLapValue]
                        resultDict[error] += [originValue]
                    else:
                        resultDict[error] = list()
                        resultDict[error] += [overLapValue]
                        resultDict[error] += [originValue]
                    del (resultDict[target])
                else:
                    pass
        else:
            print(overLapValue)
            resultDict[target] = overLapValue

    def makePedi(idv, kpn, mom, birth, sex):
        pedi = []
        idv = str(idv)
        kpn = str(kpn)
        mom = str(mom)
        birth = str(birth)
        sex = str(sex)
        pedi.append(idv.replace(" ", ""))
        pedi.append(kpn)
        pedi.append(mom.replace(" ", ""))
        pedi.append(birth.replace(" ", ""))
        pedi.append(sex.replace(" ", ""))
        return pedi

    def birthSexValue(target, keyDict):
        idvInfo = openApiParser(target, keyDict)
        birth = ExtractValueOpenApi(idvInfo, 'birthYmd')
        if str(birth) == 'None':
            pass
        else:
            birth = birth[0:4] + "-" + birth[4:6] + "-" + birth[6:]
        sex = ExtractValueOpenApi(idvInfo, 'sexNm')
        return birth, sex

    oneStopSoup = oneStopParser(target)

    idvNumber = idvInfoValue(oneStopSoup, 0, 0)

    dataCheck = len(idvNumber)

    if dataCheck != 0:
        pediInfo = extractTargetTable(oneStopSoup, 'contact08', 0)

        kpn = extractTargetValue(pediInfo, 2, 1)
        mom = extractTargetValue(pediInfo, 5, 3)
        birth, sex = birthSexValue(target, keyDict)

        idvPedi = makePedi(target, kpn, mom, birth, sex)
        try:
            overlapCheck(resultDict, idvPedi[0], idvPedi)
        except AttributeError:
            error = 'error'
            print('개체에러')
            if error in resultDict:
                resultDict[error] += [idvPedi]
            else:
                resultDict[error] = list()
                resultDict[error] += [idvPedi]
                pass

        if mom is not None:
            motherKpn = extractTargetValue(pediInfo, 4, 1)
            motherMom = extractTargetValue(pediInfo, 6, 3)
            momBirth, momSex = birthSexValue(idvPedi[2], keyDict)

            motherPedi = makePedi(mom, motherKpn, motherMom, momBirth, momSex)
            try:
                overlapCheck(resultDict, motherPedi[0], motherPedi)
            except AttributeError:
                error = 'error'
                print('개체에러')
                if error in resultDict:
                    resultDict[error] += [motherPedi]
                else:
                    resultDict[error] = list()
                    resultDict[error] += [motherPedi]
                    pass
        else:
            pass

        farrowInfo = extractTargetTable(oneStopSoup, 'contact05', 1)

        if len(farrowInfo[1].find_all('td')) != 1:  # 개체의 분만데이터 존재 여부를 판별함.
            for column in range(len(farrowInfo)-1):
                farrowIdvNm = extractTargetValue(farrowInfo, column+1, 5)
                if len(farrowIdvNm) < 12:
                    pass
                else:
                    farrowIdvNm = str(farrowIdvNm).replace(" ", "")
                    farrowKpn = extractTargetValue(farrowInfo, column+1, 2)
                    farrowIdvBirth, farrowIdvSex = birthSexValue(
                        farrowIdvNm, keyDict)
                    farrowPedi = makePedi(
                        farrowIdvNm, farrowKpn, target, farrowIdvBirth, farrowIdvSex)
                    try:
                        overlapCheck(resultDict, farrowPedi[0], farrowPedi)
                    except AttributeError:
                        error = 'error'
                        print(f'개체에러{farrowPedi}')
                        if error in resultDict:
                            resultDict[error] += [farrowPedi]
                        else:
                            resultDict[error] = list()
                            resultDict[error] += [farrowPedi]
                            pass
        else:
            pass  # 개체의 분만(후대)데이터가 없으므로 pass
    else:
        birth, sex = birthSexValue(target, keyDict)
        errorPedi = makePedi(target, 'None', 'None', birth, sex)
        error = 'error'
        print(f'개체에러{errorPedi}')
        if error in resultDict:
            resultDict[error] += [errorPedi]
        else:
            resultDict[error] = list()
            resultDict[error] += [errorPedi]
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

    pool = multiprocessing.Pool(processes=10)

    pool.starmap(dataCrl, [(cow, CrlResult, keyDict) for cow in cowList])

    pool.close()

    pool.join()

    error = 'error'
    if error in CrlResult:
        idvErrorList = CrlResult.get(error)
        del (CrlResult[error])
    else:
        pass

    idvResultList = list(CrlResult.values())

    resultDfHeader = ["IID", "KPN", "MID", "BIR", "SEX"]
    idvResultDf = pd.DataFrame(
        idvResultList, columns=resultDfHeader, dtype='string')
    idvResultDf = idvResultDf.drop_duplicates(['IID'], keep='first')

    cowListDf = pd.DataFrame(cowList, columns=["IID"], dtype='string')
    cowListDf = pd.merge(idvResultDf, cowListDf)

    momList = set(cowListDf["MID"].unique())

    if 'None' in momList:
        momList.remove('None')
    else:
        pass

    idvList = set(cowListDf["IID"].unique())

    momCrlList = list(momList - idvList)

    pool = multiprocessing.Pool(processes=10)

    pool.starmap(dataCrl, [(cow, CrlResult, keyDict) for cow in momCrlList])

    pool.close()

    pool.join()

    if error in CrlResult:
        momErrorList = CrlResult.get(error)
        del (CrlResult[error])
    else:
        pass

    crlResultList = list(CrlResult.values())

    resultDfHeader = ["IID", "KPN", "MID", "BIR", "SEX"]
    crlResultDf = pd.DataFrame(
        crlResultList, columns=resultDfHeader, dtype='string')
    crlResultDf = crlResultDf.drop_duplicates(['IID'], keep='first')

    if 'idvErrorList' in vars():
        idvErrorDf = pd.DataFrame(
            idvErrorList, columns=resultDfHeader, dtype='string')
    if 'momErrorList' in vars():
        momErrorDf = pd.DataFrame(
            momErrorList, columns=resultDfHeader, dtype='string')

    with pd.ExcelWriter(f'개체_어미_혈통결과.xlsx') as writer:

        crlResultDf.to_excel(writer, sheet_name="혈통결과", index=False)

        if 'idvErrorDf' in vars():
            idvErrorDf.to_excel(writer, sheet_name="개체크롤링오류", index=False)
        if 'momErrorDf' in vars():
            momErrorDf.to_excel(writer, sheet_name="어미크롤링오류", index=False)

    print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간

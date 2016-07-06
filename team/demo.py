#!/usr/bin/python
# -*- coding: utf-8 -*-

 
import json;
 
def my_json():
    print "--- 1. Demo json to object value (here is dict) ---";
    inputJsonStr = """{"list":[{"ArticleId":7392749,"BlogId":1158641,"CommentId":2182295,"Content":"test","ParentId":0,"PostTime":"2","Replies":null,"UserName":"crifan","Userface":"http://1.jpg"},{"ArticleId":7392749,"BlogId":1158641,"CommentId":2182254,"Content":"mark__","ParentId":0,"PostTime":"52","Replies":null,"UserName":"mars_tl","Userface":"http://2.jpg"}],"page":{"PageSize":20,"PageIndex":1,"RecordCount":0,"PageCount":0},"fileName":"7392749"}""";
    #print "inputJsonStr=",inputJsonStr;
    convertedDict = json.loads(inputJsonStr, "utf-8");
    print "type(convertedDict)=",type(convertedDict); #type(convertedDict)= <type 'dict'>
    print "convertedDict=",convertedDict;
 
    # convertedDict= {u'list': [{u'UserName': u'crifan', u'PostTime': u'2\u5206\u949f\u524d', u'Userface': u'http://avatar.csdn.net/E/D/E/3_crifan
    # .jpg', u'Replies': None, u'Content': u'\u5076\u662f\u6765\u6d4b\u8bd5\u8bc4\u8bba\u6ef4\uff0c\u54c8\u54c8', u'ArticleId': 7392749, u'ParentI
    # d': 0, u'CommentId': 2182295, u'BlogId': 1158641}, {u'UserName': u'mars_tl', u'PostTime': u'52\u5206\u949f\u524d', u'Userface': u'http://ava
    # tar.csdn.net/9/3/0/3_mars_tl.jpg', u'Replies': None, u'Content': u'mark__', u'ArticleId': 7392749, u'ParentId': 0, u'CommentId': 2182254, u'
    # BlogId': 1158641}], u'page': {u'PageIndex': 1, u'PageCount': 0, u'PageSize': 20, u'RecordCount': 0}, u'fileName': u'7392749'}
     
    #now can output some field value
    filename = convertedDict['fileName'];
    print "filename=",filename; 
    # dictList = convertedDict['list'];
    # for eachDict in dictList:
        # print "eachDict=",eachDict;
 
    print "--- 2. Demo object value to json ---";
    jsonStr = json.dumps(convertedDict);
    print "type(jsonStr)=",type(jsonStr); #type(jsonStr)= <type 'str'>
    print "jsonStr=",jsonStr;
     
    print "=== more about json can refer: docs.python.org/2/library/json.html ===";
         
if __name__ == '__main__':
    my_json()

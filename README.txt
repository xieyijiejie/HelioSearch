Heliosearch
Solr Evolved - The next generation of open source search.

http://heliosearch.org


进入solr源码文件夹，d:\apache-ant-1.9.0\bin\ant.bat dist生成war和jar


java -Dsolr.solr.home=/data/solr-hs_0.08/example/solr -jar start.jar




readable和writable标签
设置
http://192.168.0.116:8983/solr/admin/cores?readable=true
http://192.168.0.116:8983/solr/admin/cores?readable=false
http://192.168.0.116:8983/solr/admin/cores?writable=true
http://192.168.0.116:8983/solr/admin/cores?writable=false
读取配置
http://192.168.0.116:8983/solr/admin/cores

jsonjoin
{!jsonjoin}{$or:[{index:metrix_hcp, q:"province:13"}, {$join:{from:{index:metrix_organization, q:"type:1"}, to:{index:metrix_hcp, q:"type_list:1"}, on:"id->institution_list"}}]}
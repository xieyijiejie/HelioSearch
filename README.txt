Heliosearch
Solr Evolved - The next generation of open source search.

http://heliosearch.org


����solrԴ���ļ��У�d:\apache-ant-1.9.0\bin\ant.bat dist����war��jar


java -Dsolr.solr.home=/data/solr-hs_0.08/example/solr -jar start.jar




readable��writable��ǩ
����
http://192.168.0.116:8983/solr/admin/cores?readable=true
http://192.168.0.116:8983/solr/admin/cores?readable=false
http://192.168.0.116:8983/solr/admin/cores?writable=true
http://192.168.0.116:8983/solr/admin/cores?writable=false
��ȡ����
http://192.168.0.116:8983/solr/admin/cores

jsonjoin
{!jsonjoin}{$or:[{index:metrix_hcp, q:"province:13"}, {$join:{from:{index:metrix_organization, q:"type:1"}, to:{index:metrix_hcp, q:"type_list:1"}, on:"id->institution_list"}}]}
# Graph database for Galaxy tools

### Installation instructions:
- https://neo4j.com/docs/operations-manual/current/installation/linux/debian/#debian-installation
- https://dzone.com/articles/installing-neo4j-on-ubuntu-1604

### Desktop software:
- https://neo4j.com/download-thanks-desktop/?edition=desktop&flavour=unix&release=1.2.1&offline=true

### Steps for installing Neo4j:

#### Install java
1. ```sudo apt install default-jre```

#### Check Java's version
- ```java --version```

#### Install Neo4j
2. ```wget -O - https://debian.neo4j.org/neotechnology.gpg.key | sudo apt-key add -```
3. ```echo 'deb https://debian.neo4j.org/repo stable/' | sudo tee -a /etc/apt/sources.list.d/neo4j.list```
4. ```sudo apt-get update```
5. ```sudo apt-get install neo4j=1:3.5.12```

#### Uncomment the line "dbms.connectors.default_listen_address=0.0.0.0" present at "/etc/neo4j/neo4j.conf"

#### Start/Stop the Neo4j service
- ```sudo service neo4j start```
- ```sudo service neo4j stop```
- ```sudo service neo4j restart```

#### Open in browser:
- http://127.0.0.1:7474/browser/

#### Install requirements
- ```conda create --name graph_db python=3.6```
- ```pip install neo4j``` (optional)
- ```pip install py2neo```
- ```conda install requests```

#### More details:
- https://dzone.com/articles/installing-neo4j-on-ubuntu-1604
- https://medium.com/@Jessicawlm/installing-neo4j-on-ubuntu-14-04-step-by-step-guide-ed943ec16c56
- https://datawookie.netlify.com/blog/2016/09/installing-neo4j-on-ubuntu-16.04/

#### Reading manual:
- https://neo4j.com/docs/developer-manual/3.2/introduction/

<h1>brain</h1>
This repo contains the Coincube Brain Module.


<h2>Getting Started</h2>
In order to get up and running with this repo:

<h2>Database Submodule</h2>
This repo contains one submodule which needs to be pulled down before you will be able to work.<br>

<h3>In order to pull in the submodule you will need to run:</h3>
`git submodule init`<br>
`git submodule update`<br>

<h3>Please be sure to push submodule changes to remote</h3>

<h5>Git Submodule Tutorial <a href="https://git-scm.com/book/en/v2/Git-Tools-Submodules">#1</a></h5>
<h5>Git Submodule Tutorial <a href="https://git.wiki.kernel.org/index.php/GitSubmoduleTutorial">#2</a></h5>


<h2>Docker Setup</h2>
You will need <a href="https://docker.com" target="_blank">Docker</a>.

Build the Docker container(s):
For local environment: `docker-compose build`<br>
For production environment: `docker-compose -f docker-compose.prod.yml build`<br>

Run the Docker container(s):
For local environment: `docker-compose up`<br>
For production environment: `docker-compose -f docker-compose.prod.yml up -d`

<h3>Shell into a specific container</h3>
Find "CONTAINER ID": `docker ps`<br>

Shell into container: `docker exec -it "CONTAINER ID" bash`<br>
(i.e. `docker exec -it 78e539ca25be bash`)

<h3>View container logs</h3>
`docker logs -f "CONTAINER ID"`
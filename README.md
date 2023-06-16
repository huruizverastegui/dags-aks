# Set up of the FDTN Infrastructure

## Main needs: 
- Pipeline orchestrator
- Postgres database for anything georelated
- Low cost data storage  
- App hosting
- Computing power for intermittent projects 

We chosed to go with Azure as this is the Cloud Provided our Valencia team is the most familiar with and we should be able to transition it to them as soon as we have some budget 

### Pipeline orchestrator : Airflow

Decided to go with Airflow as it's widely used open source tool and the team is already familiar with it. 
There were several options within Azure: 
- Managed airflow within the Azure Data Factory - but too expensive for our limited budget - however could be interesting later on if we scale up 
- Direct deployment via Docker container - tried it and run into several errors as it was more complex 
- Deployment via Kubernetes and Helm chart - this was the easiest and most documented way - possible to directly deploy the official helm chart 

### Deploy fronted Sitrep

#create the resource group 
az group create --name sitrep_registry --location eastus

#create the webplan to host the apps
az appservice plan create \
--name webplan \
--resource-group sitrep_registry \
--sku B1 \
--is-linux

### Deploy backend Sitrep

CD into the folder containing the code 

#not needed if the resource group has already been created 
az group create --name sitrep_registry --location eastus

#create the container registry
az acr create --resource-group sitrep_registry --name sitrepback --sku Basic --admin-enabled true

ACR_PASSWORD=$(az acr credential show \
--resource-group sitrep_registry \
--name sitrepback \
--query "passwords[?name == 'password'].value" \
--output tsv)

#build the docker image
az acr build \
  --resource-group sitrep_registry \
  --registry sitrepback \
  --image sitrepback:latest .

#deploy the web app
#the container password needs to be retrieved from the access keys in azure UI
az webapp create \
--resource-group sitrep_registry \
--plan webplan --name sitrepapi \
--docker-registry-server-password <containerpassword> \
--docker-registry-server-user sitrepback \
--role acrpull \
--deployment-container-image-name sitrepback.azurecr.io/sitrepback:latest

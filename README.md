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


## Set up an Kubernetes cluster 
Free services > Azure Kubernetes Service (AKS) - Create 
Preset configuration : Cost-optimized 

## On that cluster -  install Airflow : 

documentation: https://github.com/airflow-helm/charts/blob/main/charts/airflow/docs/guides/quickstart.md

Connect to the aks cluster you just created by navigating to it and selecting connect
Via the terminal : 
'az login' to login into azure
Follow the instructions from the 'Azure CLI' tab : 'az account set --subscription xxx' and later 'az aks get-credentials --resource-group xxx'

type in the following : 

-- set the release-name & namespace
export AIRFLOW_NAME="airflow-cluster"
export AIRFLOW_NAMESPACE="airflow-cluster"

-- create the namespace
kubectl create ns "$AIRFLOW_NAMESPACE"

-- install using helm 3
helm install \
  "$AIRFLOW_NAME" \
  airflow-stable/airflow \
  --namespace "$AIRFLOW_NAMESPACE" \
  --version "8.X.X" \
  --values ./values_stable.yaml
  
-- wait until the above command returns and resources become ready 
 

## Use this adapted stable_values.yaml file 
Main changes vs the usual one available here: 
- Connection via git sync to the git repo containing the dags and sql files
- additional requirements.txt
- Change of the node type to load balancer for the web server to be able to connect externally to it

- Make sure to have a public IP to assign to the load balancer 
- decrease the volume size 



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

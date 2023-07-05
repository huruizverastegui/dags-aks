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

Airflow helm chart documentation: https://github.com/airflow-helm/charts/blob/main/charts/airflow/docs/guides/quickstart.md

Connect to the aks cluster you just created by navigating to it and selecting connect
Via the terminal : 

Git copy this repo into your machine and cd into the folder containing the values_stable.yaml file

type 'az login' to login into azure
Follow the instructions from the 'Azure CLI' tab : 'az account set --subscription xxx' and later 'az aks get-credentials --resource-group xxx'

type in the following : 
-- set the release-name & namespace
```
export AIRFLOW_NAME="airflow-cluster"
export AIRFLOW_NAMESPACE="airflow-cluster"
```
-- create the namespace
```kubectl create ns "$AIRFLOW_NAMESPACE"```

-- install using helm 3
```helm install \
  "$AIRFLOW_NAME" \
  airflow-stable/airflow \
  --namespace "$AIRFLOW_NAMESPACE" \
  --version "8.X.X" \
  --values ./values_stable.yaml
 ``` 
-- wait until the above command returns and resources become ready 
 

## Use this adapted stable_values.yaml file 
Main changes vs the usual one available here https://github.com/airflow-helm/charts/blob/main/charts/airflow/values.yaml : 
- row 31: changed the fernet key
- row 37: changed the webserver SecretKey
- row 74: changed the admin login 
- row 119-136 : added a connection to our Postgres
- row 218: added the pip packages to be installed in all pods
- row 814: changed the web pod from clusterIP to Load balancer and attributed a public IP created in Azure to enable external traffic on the airflow UI 
- row 1383-1410-1416-1420: enabled git sync to get the dags from a git repo
- row 1923: disabled postgres persistency as it was causing errors   


### Deploy fronted Sitrep

#create the resource group 
```az group create --name sitrep_registry --location eastus```

#create the webplan to host the apps
```
az appservice plan create \
--name webplan \
--resource-group sitrep_registry \
--sku B1 \
--is-linux
```

### Deploy backend Sitrep

CD into the folder containing the code 

#not needed if the resource group has already been created
az group create --name sitrep_registry --location eastus

#create the container registry

```az acr create --resource-group sitrep_registry --name sitrepback --sku Basic --admin-enabled true```


```ACR_PASSWORD=$(az acr credential show \
--resource-group sitrep_registry \
--name sitrepback \
--query "passwords[?name == 'password'].value" \
--output tsv)
```
#build the docker image
```az acr build \
  --resource-group sitrep_registry \
  --registry sitrepback \
  --image sitrepback:latest .
```

#deploy the web app
#the container password needs to be retrieved from the access keys in azure UI
```
az webapp create \
--resource-group sitrep_registry \
--plan webplan --name sitrepapi \
--docker-registry-server-password <containerpassword> \
--docker-registry-server-user sitrepback \
--role acrpull \
--deployment-container-image-name sitrepback.azurecr.io/sitrepback:latest
```


## Set up an automated sync from a project repo to the airflow dag repo  

-- follow the tutorial 
-- https://levelup.gitconnected.com/github-action-to-automatically-push-to-another-repository-1e327862f067
-- https://cpina.github.io/push-to-another-repository-docs/index.html

-- set up ssh keys pair:
ssh-keygen -t ed25519 -C "test@gmail.com"       

-- set up a SSH deploy key in the target repo (huruizverastegui/dags-aks) - with the public key
-- set up a git hub secret in the source repo (unicef/hd4d_ppt) - with the private key
-- create a yaml workflow file under     .github/workflows/

```
name: CI \
on: \
  push: \
    branches: [ staging ] \
    paths: \
      - 'data_engineering/**' \
jobs: \
  build: \
    runs-on: ubuntu-latest \
    steps: \
      - uses: actions/checkout@v2 \
      - name: Pushes to another repository \
        uses: cpina/github-action-push-to-another-repository@ssh-deploy-key \
        env: \
          SSH_DEPLOY_KEY: ${{ secrets.WORKFLOW_DEPLOY_KEY }} \
        with: \
          source-directory: 'data_engineering/dags/' \
          destination-github-username: 'huruizverastegui' \
          destination-repository-name: 'dags-aks' \
          target-branch: main \
          target-directory: 'dags/' \
          user-email: hugo.ruiz.verastegui@gmail.com \
```

## Continuous deployment into Azure
After deploying the app for the first time via the tutorial above (front end and back end )
Go to the app in the Azure UI > Deployment > Deployment center 
Select Github actions 
Go to the github repo > the workflow file that has been created > update the document 

## Set up user access 
Go to the Azure Active Directory 
Create 2 group : viewers / admin 
Add the relevant users in each group

Go to the subscription page 

## set up test user with password 

## set up authentification for the apps
Go to the app 


## Force https 

##

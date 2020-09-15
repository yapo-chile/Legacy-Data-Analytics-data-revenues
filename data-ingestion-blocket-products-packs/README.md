# data-ingestion-blocket-products-packs pipeline 

# data-ingestion-blocket-products-packs
This pipeline make ingestion of data from Blocket DB to Data Warehouse, to be used by analitics
## Description
This pipeline consist of three parts that can be found in usecases

## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | Added in bottom list                                                       |
| Output Source     | Added in bottom list                                                       |
| Schedule          | 00:10                                                                      |
| Rundeck Access    | data-jobs in RAW - Data ingestion blocket - Packs & Products               |
| Associated Report | NA                                                                         |

### Source
- Read:
	- Blocket:
		- pmnt_api_products
		- purchase_detail
		- purchase
		- payment_groups
		- packs
		- tokens
		- admins
		- accounts
		- ads
		- pack_params
		- public.purchase_in_app
		- purchase_detail_params
	- DW:
		- ods.seller
		- stg.packs
		- stg.purchase_ios
		- stg.dolar_euro
		- stg.product_order_detail
### Destination (specify tables and schemas)
- Insert
	- DW:
		- stg.temp_pack         
		- stg.pack_manual_accepted
		- stg.ads_disabled_pack_autos
		- stg.packs
		- stg.purchase_ios
		- stg.product_order_detail
		- ods.packs
		- ods.product_order_ios
		- ods.product_order_detail
- Delete:
	- DW:
		- ods.packs


### Build
```
make docker-build
```

### Run micro services
```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/data-ingestion-blocket-products-packs:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/data-ingestion-blocket-products-packs:[TAG] \
           -date_from=YYYY-MM-DD \
           -date_to=YYYY-MM-DD
```

### Adding Rundeck token to Travis

If we need to import a job into Rundeck, we can use the Rundeck API
sending an HTTTP POST request with the access token of an account.
To add said token to Travis (allowing travis to send the request),
first, we enter the user profile:
```
<rundeck domain>:4440/user/profile
```
And copy or create a new user token.

Then enter the project settings page in Travis
```
htttp://<travis server>/<registry>/<project>/settings
```
And add the environment variable RUNDECK_TOKEN, with value equal
to the copied token

# gcp-cf-fin-crawl-test

## deploy cloud function

```sh
$ cd deploy/terraform
$ terraform init
$ terraform plan
$ terraform apply
```

required roles for service account

- Cloud Functions管理者
- Pub/Sub管理者
- サービスアカウントユーザー
- ストレージ管理者
- Cloud Scheduler管理者
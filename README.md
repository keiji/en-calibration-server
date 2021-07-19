# Exposure Notifications Simple Server

## Setup

### Requirements

 * golang 1.16
 * openssl
 * Python 3
 * Python3-pip
 * uwsgi

### Generate signing-key

```
openssl ecparam -genkey -name prime256v1 -noout -out private.pem
```

### Build generate-export

```
git submodule update
cd exposure-notifications-server/tools/export-generate
go build
```

### Edit `config.json`

```
{
  "region": 440,
  "base_url": "https://en.keiji.dev/diagnosis_keys/",
  "db_uri": "sqlite:////tmp/en/test.db",
  "base_path": "/tmp/en",
  "export-generate_bin_path": "/home/ubuntu/en-calibration-server/exposure-notifications-server/tools/export-generate/export-generate",
  "signing_key_path": "/home/ubuntu/private.pem"
}
```

### Install requirements

```
pip3 -r server/requirements.txt
```

## How to use

### Start server(uwsgi)

```
cd server
CONFIG_PATH=sample/config.json \
    uwsgi --ini uwsgi.ini
```

### Put diagnosis-keys [from client]

```
curl -T sample/diagnosis_keys.json https://en.keiji.dev/diagnosis_keys/012345/
```

```
12 diagnosis_keys have been added.
```

### Generate diagnosis-keys packages

```
CONFIG_PATH=sample/config.json \
	python3 generate_diagnosis_keys.py
```

```
12 new diagnosis-keys have been found.
2021/07/18 15:17:28 Using TEKs provided in: /tmp/en/012345/diagnosis_keys-mpdysnkb.json
2021/07/18 15:17:28 number of batches: 1
2021/07/18 15:17:28 Creating /tmp/en/012345/diagnosis_keys-mpdysnkb-12-records-1-of-1.zip
```

### Get diagnosis-keys list

```
curl https://en.keiji.dev/diagnosis_keys/012345/list.json
```

```
[
    {
        "region": 440,
        "url": "https://en.keiji.dev/diagnosis_keys/012345/diagnosis_keys-mpdysnkb-12-records-1-of-1.zip",
        "created": 1626589048
    }
]
```

### Get diagnosis-keys

```
curl -O https://en.keiji.dev/diagnosis_keys/012345/diagnosis_keys-mpdysnkb-12-records-1-of-1.zip
```

### Setup a cron job

```
# m h  dom mon dow   command
*/10 * * * * ~/en-calibration-server/server/sample/generate_diagnosis_keys.sh
```

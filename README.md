# DBF

## Description
DBF(DB Factory) is a general-purpose test data generator for both PostgreSQL and MySQL.

## Features
* **Supports both PostgreSQL and MySQL.**
* **Supports Linux, MacOS, and Windows.**
* **High performance.**
* **Multiply methods to generate random data.**

## Install

```bash
go get github.com/myzhan/dbf
```

It will install an executable binary under $PATH/bin.

## Build

```bash
# xxx means your os
bash build_xxx.sh
```

## Usage

1. Initialize a configuration file, defaults to conf.json

```bash
cp conf.json.sample conf.json
```

2. Dump schema

```bash
dbf --op dump
```

The schema will be write to a new file named ${table}_schema.json

3. Specify mutators of columns

```bash
vim ${table}_schema.json
```

4. Insert data

```bash
dbf --op insert
```
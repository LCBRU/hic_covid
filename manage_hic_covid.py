#!/usr/bin/env python
from migrate.versioning.shell import main
from environment import HIC_CONNECTION_STRING

if __name__ == '__main__':
    main(url=HIC_CONNECTION_STRING, repository='hic_covid_repository', debug='False')

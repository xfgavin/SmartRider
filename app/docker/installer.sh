#!/usr/bin/env bash
cd /tmp
echo "**********************************************"
echo "*Installing prerequisites                    *"
echo "**********************************************"
apt-get -qq update >/dev/null 2>&1
apt-get install -qq --no-install-recommends --allow-unauthenticated git >/dev/null 2>&1
echo "**********************************************"
echo "*Installing smartrider app                   *"
echo "**********************************************"
git clone https://github.com/xfgavin/SmartRider.git
mv SmartRider/app/assets /usr/src/app
mv SmartRider/app/smartrider_app.py /usr/src/app
mv SmartRider/app/docker/smartrider_entry.sh /usr/src/app
#sed -e "s/os.environ\['PGHOST'\]/'192.168.185.62'/g" -e "s/os.environ\['PG.*'\]/'smartrider'/g" -i /usr/src/app/smartrider_app.py
rm -f /usr/src/app/requirements.txt
echo "**********************************************"
echo "*Cleaning up                                 *"
echo "**********************************************"
apt-get -qq purge git
apt-get -qq autoremove >/dev/null
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
chmod -R 777 /tmp
rm -f /bin/sh
ln -s bash /bin/sh

# SysPAD Monitor

SysPAD Monitor permet de surveiller et d'être notifié par des capteurs de fournisseurs tiers
 et de mettre à jour la base de données patients de SysPAD.

# Installation d'une nouvelle machine

## Clonage
### 1/ Récupérer l'image sur mafalda
L'image actuelle est dans /home/debian/2020-11-17-13-img

C'est un répertoire, le récupérer par rsync par exemple:
`rsync -avz --progress debian@mafalda.mobaspace.com:~/2020-11-17-13-img ./`
### 2/ Cloner l'image
Pour cela, utiliser Clonezilla sur une clé bootable.
`https://clonezilla.org/clonezilla-live.php`

### 3/ Démarrer la machine
Attention, il faut démarrer la machine sans la connecter au réseau.

## Configuration
### 1/ Créer un client vpn
Sur mafalda.mobaspace.com, connecté avec l'utilisateur debian:
`sudo ./wireguard-install.sh`

choisir `1) Add a new user`
Comme nom de client indiquer px, ou x est la fin de l'ip qui sera attribuée,
 par exemple `p5` qui donnera l'ip `10.66.66.5`
 
 Cela va créer un fichier du type `wg0-client-p5.conf`
 
 C'est ce fichier qu'il faut récupérer pour l'installer sur la nouvelle machine.
 
 ### 2/ Créer un fichier de configuration Apache
 Sur mafalda.mobaspace.com, 
 copier `/etc/apache2/sites-available/demo.conf` vers le nouveau fichier.
 
 Par exemple :
 `sudo cp /etc/apache2/sites-available/demo.conf /etc/apache2/sites-available/lesjardinsderambam.conf` 
 
 Éditer ce fichier et changer les lignes:
  - `ServerName demo.mobaspace.com`
  - `redirect 302 / https://demo.mobaspace.com/`
  - `ProxyPassReverse / http://10.66.66.2:5100/`
  - `RewriteRule ^/(.*) http://10.66.66.2:5100/$1 [P,L]`
 
 Activer le site :
  - `sudo a2ensite lesjardinsderambam.conf`
  - `sudo systemctl reload apache2`
  
 
 ### 3/ Vérifier les fichiers de configuration
 Sur la machine :
 - Copier le fichier vpn client vers `/etc/wireguard/wg0-client-demo.conf` (garder ce nom)
 Les fichiers à vérifier, modifier sont :
 - `/etc/syspad_monitor.conf`
 - `/var/www/mobaspace/appsettings.json`
 - Vider la base de données
 
 Concernant la base de données, normalement un truncate cascade sur les tables Patients, Capteurs et OAuth2Apis devrait convenir.
 
 Sur mafalda pour le déploiement automatique des logiciels est fait par:
 
 - `/home/exploit/syspad-deploy.sh`
 - `/home/exploit/mobaspace-deploy.sh`
 
 Ces scripts lisent les fichiers `wg0-client-*.conf` qui sont dans `/home/debian`, 
 plus besoin donc de modifier les scripts à chaque installation.
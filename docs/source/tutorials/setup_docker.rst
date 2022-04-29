.. _setting_up_docker:

Setting up Docker and VS Code
-----------------------------------------
This tutorial goes over installing Docker and setting up a Docker container 
in VSCode.

Start by installing `Docker <https://www.docker.com/products/docker-desktop>`_ 
and `VSCode <https://code.visualstudio.com/>`_. It's a good idea to familiarize
yourself with the VScode interface, and feel free to go through
Docker's initial tutorial after installation. It goes through basics on setting 
up a Docker "engine" to make sure it's working - you don't need to know how to 
do this (or really anything in Docker) to use Tsdat.

[Note: if you want to uninstall Docker, there is a series of steps to completely
`remove it <https://docs.microsoft.com/en-us/virtualization/windowscontainers/manage-docker/configure-docker-daemon>'_.

Once you have VS Code and Docker downloaded and installed:

1. Open VSCode -> New Window -> Open Folder -> open cloned template folder ("ncei_global_marine_data_ingest")
	
  .. figure:: global_marine_data/vscode1.png
      :align: center
      :width: 100%
      :alt:

  |

  .. figure:: global_marine_data/vscode2.png
      :align: center
      :width: 100%
      :alt:

  |
	
2. VSCode will prompt you if you want to open in Docker -> Click yes and wait for docker to initiate, which takes a minute or two.
	
  .. figure:: global_marine_data/vscode3.png
      :align: center
      :width: 100%
      :alt:

  |

  .. figure:: global_marine_data/vscode4.png
      :align: center
      :width: 100%
      :alt:

  |
	
3. VSCode will prompt if you want to install dependencies -> Hit install; you can close the new windows it opens
	
  .. figure:: global_marine_data/vscode5.png
      :align: center
      :width: 100%
      :alt:

  |

4. VS Code will then prompt you to restart window after pylance is installed -> Hit yes again and VS Code will reboot
	
  .. figure:: global_marine_data/vscode6.png
      :align: center
      :width: 100%
      :alt:

  |

Congrats! Python environment handling done. 

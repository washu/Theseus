#!/bin/bash
#
# A script to push changes from EloquentRaft to GitHub via a sync'd Theseus
# repository.
#
# Author:        Gabor Angeli <gabor@eloquent.ai>
# Created:       March 18 2013
# Last modified: June     27 2013 (by Gabor)
# Last modified: January  21 2013 (by Gabor -- Add .gitignore and ignore classes/ directory)
# Last modified: September 6 2018 (by Zames) -- Edit script to be used for Eloquent Theseus


##################
# Configure bash #
##################

# Exit script on program fail
set -o errexit
# Don't run commands if they are undefined
set -o nounset


####################
# ensureCmd Checks #
####################

echo "----- Doing checks -----"

# Import useful utils from main.sh
PRG="$BASH_SOURCE"
progname=`basename "$BASH_SOURCE"`
while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`"/$link"
    fi
done
ROOT="`realpath $(dirname "$PRG")/../..`"
source "$ROOT/bin/main.sh"
# Check if external commands exist
ensureCmd ssh-agent
ensureCmd make
ensureCmd rsync


########################
# INITIALIZE VARIABLES #
########################

echo "----- Initializing variables -----"

# Timer to keep track of duration of the script
TIMER_START="$SECONDS"

# Base directory where the job is being run
# By default, this uses $CI_PROJECT_DIR which is the folder where GitLab runs this script
BASE_DIR="$CI_PROJECT_DIR"

# The directory with the current Theseus checkout
# Optimally, this directory should be tested and should compile
# This directory name should not have spaces in it
# e.g., SOURCE_DIR="/user/angeli/workspace/nlp/"
SOURCE_DIR="$BASE_DIR/eloquent/public"

# Create a temporary directory which will get sync'd to GitHub
# This directory name should not have spaces in it
# e.g., CORENLP_DIR="/user/angeli/tmp/corenlp-github"
THESEUS_DIR="$BASE_DIR/theseus"

# Create a temporary directory to store our metadata
# This directory name should not have spaces in it
# e.g., CORENLP_DIR="/user/angeli/tmp/corenlp-github"
GITHUB_META_DIR="$BASE_DIR/github_meta"

# The path to the Github repo
# e.g., GITHUB_REPO_PATH="git@github.com:gangeli/CoreNLP-pilot.git"
GITHUB_REPO_PATH="git@github.com:eloquentlabs/Theseus.git"


#####################
# Utility functions #
#####################

function ensureCleanState {
  if [ -d "$THESEUS_DIR" ]; then
    rm -rf "$THESEUS_DIR"
  fi
  if [ -d "$GITHUB_META_DIR" ]; then
    rm -rf "$GITHUB_META_DIR"
  fi
}

# Delete the temporary folders that we created
function doCleanup {
echo "----- Performing cleanup -----"
  if [ -d "$THESEUS_DIR" ]; then
    rm -rf "$THESEUS_DIR"
  fi
  if [ -d "$GITHUB_META_DIR" ]; then
    rm -rf "$GITHUB_META_DIR"
  fi
echo "----- Cleanup complete -----"
}

# Run our cleanup on exit, even if the script fails
trap doCleanup EXIT

# Copy files from $SOURCE_DIR to $THESEUS_DIR
# Exclude .git from being overwritten in $THESEUS_DIR or else we won't be able to push to github
function doRsync {
  echo "----- RSync (eloquent/public->theseus)-----"
  rsync \
    --archive\
    --copy-links\
    --copy-dirlinks\
    --del\
    --force\
    --recursive\
    --exclude="/.git"\
    $SOURCE_DIR/ $THESEUS_DIR/
}


################
# Setup folder #
################


# Ensure that /theseus and /github_meta does not exist yet
ensureCleanState

# Create a folder to store our Github metadata
mkdir "$GITHUB_META_DIR"

# The SSH key to push to GitHub with
# Copy SSH key from Gitlab into a temporary file
echo "$THESEUS_SSH_KEY" > "$GITHUB_META_DIR/ssh_key"

# This file name should not have spaces in it
# e.g., SSH_KEY_FILE="/user/angeli/tmp/ssh_key"
SSH_KEY_FILE="$GITHUB_META_DIR/ssh_key"
# Change access rights to our SSH key file to be read-only (If not ssh-agent won't run)
chmod 400 "$SSH_KEY_FILE"

###############
# Main script #
###############

# Download and clear the Theseus repository
ssh-agent bash -c "ssh-add $SSH_KEY_FILE; git clone $GITHUB_REPO_PATH $THESEUS_DIR"
# Copy files from eloquent/public
doRsync

cd "$BASE_DIR/eloquent"
# Clean all dependencies off our repo
make clean
# Grab the last commit message
COMMIT_MESSAGE=$(git log -1 --pretty=%B)

# Add and commit to git
echo "----- Commiting files -----"
cd $THESEUS_DIR
git add .
git commit --all --author="Gabor Angeli<gabor@eloquent.ai>" --message="$COMMIT_MESSAGE" # note: "--all" = "-a"
# Push to git
echo "----- Pushing to github-----"
ssh-agent bash -c "ssh-add $SSH_KEY_FILE; git pull"
ssh-agent bash -c "ssh-add $SSH_KEY_FILE; git push origin master"


###########
# Cleanup #
###########

doCleanup
DURATION=$(( $SECONDS - TIMER_START ))
echo "Sync to Github completed in $DURATION seconds"
echo "----- DONE -----"
exit 0

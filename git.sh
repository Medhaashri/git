(base) medhaashriv@AGLS-MLT-562 ~ % git clone https://github.com/Medhaashri/git.git
Cloning into 'git'...
remote: Enumerating objects: 3, done.
remote: Counting objects: 100% (3/3), done.
remote: Total 3 (delta 0), reused 0 (delta 0), pack-reused 0
Receiving objects: 100% (3/3), done.
(base) medhaashriv@AGLS-MLT-562 ~ % cd git
(base) medhaashriv@AGLS-MLT-562 git % nano git.py
(base) medhaashriv@AGLS-MLT-562 git % git add .
(base) medhaashriv@AGLS-MLT-562 git % git commit -m "Medhaa Shri"
[main 0e2e486] Medhaa Shri
 Committer: Medhaashri V <medhaashriv@AGLS-MLT-562.local>
Your name and email address were configured automatically based
on your username and hostname. Please check that they are accurate.
You can suppress this message by setting them explicitly. Run the
following command and follow the instructions in your editor to edit
your configuration file:

    git config --global --edit

After doing this, you may fix the identity used for this commit with:

    git commit --amend --reset-author

 1 file changed, 1 insertion(+)
 create mode 100644 git.py
(base) medhaashriv@AGLS-MLT-562 git % git push
Username for 'https://github.com': Medhaashri
Password for 'https://Medhaashri@github.com': 
Enumerating objects: 4, done.
Counting objects: 100% (4/4), done.
Delta compression using up to 8 threads
Compressing objects: 100% (2/2), done.
Writing objects: 100% (3/3), 304 bytes | 304.00 KiB/s, done.
Total 3 (delta 0), reused 0 (delta 0), pack-reused 0
To https://github.com/Medhaashri/git.git
   2732be5..0e2e486  main -> main
(base) medhaashriv@AGLS-MLT-562 git % nano git.py                
(base) medhaashriv@AGLS-MLT-562 git % git add . 
(base) medhaashriv@AGLS-MLT-562 git % git commit -m "SHRI"
[main b7781bd] SHRI
 Committer: Medhaashri V <medhaashriv@AGLS-MLT-562.local>
Your name and email address were configured automatically based
on your username and hostname. Please check that they are accurate.
You can suppress this message by setting them explicitly. Run the
following command and follow the instructions in your editor to edit
your configuration file:

    git config --global --edit

After doing this, you may fix the identity used for this commit with:

    git commit --amend --reset-author

 1 file changed, 2 insertions(+)
(base) medhaashriv@AGLS-MLT-562 git % git push
Enumerating objects: 5, done.
Counting objects: 100% (5/5), done.
Delta compression using up to 8 threads
Compressing objects: 100% (2/2), done.
Writing objects: 100% (3/3), 312 bytes | 312.00 KiB/s, done.
Total 3 (delta 0), reused 0 (delta 0), pack-reused 0
To https://github.com/Medhaashri/git.git
   0e2e486..b7781bd  main -> main
(base) medhaashriv@AGLS-MLT-562 git % git checkout develop
error: pathspec 'develop' did not match any file(s) known to git
(base) medhaashriv@AGLS-MLT-562 git % git checkout -b develop
Switched to a new branch 'develop'
(base) medhaashriv@AGLS-MLT-562 git % git branch
* develop
  main
(base) medhaashriv@AGLS-MLT-562 git % git add .
(base) medhaashriv@AGLS-MLT-562 git % git commit -m "git"
On branch develop
nothing to commit, working tree clean
(base) medhaashriv@AGLS-MLT-562 git % git pull
There is no tracking information for the current branch.
Please specify which branch you want to merge with.
See git-pull(1) for details.

    git pull <remote> <branch>

If you wish to set tracking information for this branch you can do so with:

    git branch --set-upstream-to=origin/<branch> develop

(base) medhaashriv@AGLS-MLT-562 git % git checkout -b main   
fatal: A branch named 'main' already exists.
(base) medhaashriv@AGLS-MLT-562 git % git checkout main   
Switched to branch 'main'
Your branch is up to date with 'origin/main'.
(base) medhaashriv@AGLS-MLT-562 git % git pull develop    
fatal: 'develop' does not appear to be a git repository
fatal: Could not read from remote repository.

Please make sure you have the correct access rights
and the repository exists.
(base) medhaashriv@AGLS-MLT-562 git % git pull develo 

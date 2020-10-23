package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"regexp"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/blang/semver/v4"
	"github.com/pkg/errors"
)

var (
	app     = kingpin.New("bump", "Bump changes the version of a Replicache library and updates all required files.")
	rootDir = app.Flag("root", "Path to the root of the library repository").Required().ExistingDir()
	library = app.Arg("library", "The library to bump").Required().Enum("diff-server", "repc", "replicache-sdk-js")
	version = app.Arg("version", "Version to update to.").Required().String()
)

func main() {
	err := impl()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
}

func impl() error {
	kingpin.MustParse(app.Parse(os.Args[1:]))

	v, err := semver.Make(*version)
	if err != nil {
		return errors.Wrap(err, "Invalid version")
	}

	oldVersion, err := updateVersionFile(*rootDir, v)
	if err != nil {
		return errors.Wrap(err, "Could not update VERSION")
	}

	fmt.Printf("Old version: %s\n", oldVersion)
	fmt.Printf("New version: %s\n", v)

	if v.EQ(oldVersion) {
		fmt.Println("New and old versions are identical. Nothing to do.")
		return nil
	} else if v.LT(oldVersion) {
		fmt.Println("WARNING: new version is smaller than old version. Carefully check changed files to be sure this is what you want.")
	}

	err = updateLicense(*rootDir, *library, oldVersion, v)
	if err != nil {
		return errors.Wrap(err, "Could not update license")
	}

	if *library == "repc" {
		err = updateCargoToml(*rootDir, v.String())
		if err != nil {
			return err
		}
		err = updateCargoLock(*rootDir, v.String())
		if err != nil {
			return err
		}
	} else if *library == "replicache-sdk-js" {
		err = updatePackageJSON(path.Join(*rootDir, "package.json"), v.String())
		if err != nil {
			return err
		}
		err = updatePackageJSON(path.Join(*rootDir, "package-lock.json"), v.String())
		if err != nil {
			return err
		}
	} else if *library == "diff-server" {
		err = updateVersionGo(*rootDir, v.String())
		if err != nil {
			return err
		}
	}

	err = commitGit(*rootDir, v)
	if err != nil {
		return errors.Wrap(err, "Could not commit release")
	}

	fmt.Println("Success! Committed version change. Don't forget to tag the release once it is has been merged.")
	return nil
}

func updateVersionFile(rootDir string, newVersion semver.Version) (semver.Version, error) {
	f := path.Join(rootDir, "VERSION")
	stuff, err := ioutil.ReadFile(f)
	if err != nil {
		return semver.Version{}, errors.Wrap(err, "Could not read VERSION")
	}
	oldVersion, err := semver.Parse(string(stuff))
	if err != nil {
		return semver.Version{}, errors.Wrapf(err, "Could not parse old version: %s", string(stuff))
	}

	err = ioutil.WriteFile(f, []byte(newVersion.String()), 0644)
	if err != nil {
		return semver.Version{}, errors.Wrap(err, "Could not write BSL.txt")
	}

	return oldVersion, nil
}

func updateLicense(rootDir, library string, oldVersion, newVersion semver.Version) error {
	p := path.Join(rootDir, "licenses", "BSL.txt")
	err := updateFile(p,
		fmt.Sprintf("Licensed Work:        %s (.*)\n", library),
		newVersion.String())
	if err != nil {
		return err
	}
	if newVersion.Major != oldVersion.Major || newVersion.Minor != oldVersion.Minor {
		fmt.Println("Major or minor component changed. Updating Change Date.")
		now := time.Now()
		err = updateFile(p, "Change Date:          (.*)\n",
			fmt.Sprintf("%d-%02d-%02d", now.Year()+2, now.Month(), now.Day()))
		if err != nil {
			return err
		}
	} else {
		fmt.Println("Patch release. Not updating Change Date.")
	}
	return nil
}

func updateCargoToml(rootDir, newVersion string) error {
	return updateFile(path.Join(rootDir, "Cargo.toml"), `version = "(.+?)"`,
		newVersion)
}

func updateCargoLock(rootDir, newVersion string) error {
	return updateFile(path.Join(rootDir, "Cargo.lock"),
		"\\[\\[package\\]\\]\nname = \"replicache-client\"\nversion = \"(.+?)\"",
		newVersion)
}

func updatePackageJSON(p, newVersion string) error {
	return updateFile(p, `"version": "(.+?)"`, newVersion)
}

func updateVersionGo(rootDir, newVersion string) error {
	return updateFile(path.Join(rootDir, "util", "version", "version.go"),
		`const v = "(.+?)"`, newVersion)
}

func updateFile(path, pattern, newVersion string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return errors.Wrapf(err, "Could not compile regexp: %s", pattern)
	}
	old, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Wrapf(err, "Could not read %s", path)
	}

	match := re.FindSubmatchIndex(old)
	if match == nil {
		return fmt.Errorf("Could not find pattern %s in %s", pattern, path)
	}

	new := old[:match[2]]
	new = append(new, []byte(newVersion)...)
	new = append(new, old[match[3]:]...)

	err = ioutil.WriteFile(path, new, 0644)
	if err != nil {
		return errors.Wrapf(err, "Could not write new %s", path)
	}

	return nil
}

func commitGit(root string, newVersion semver.Version) error {
	prevDir, err := os.Getwd()
	if err != nil {
		return err
	}
	err = os.Chdir(root)
	if err != nil {
		return err
	}
	defer os.Chdir(prevDir)

	err = ex("git", "commit", "-a", "-m", fmt.Sprintf("Bump version to %s.", newVersion))
	if err != nil {
		return err
	}
	return nil
}

func ex(args ...string) error {
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

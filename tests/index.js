const fs = require('fs');
const Path = require('path')
const globby = require('globby');
const { spawnSync } = require('child_process');

// Check for rebase parameter
let rebase = false;
const rebasePos = process.argv.indexOf("--rebase");
if (rebasePos !== -1) {
    rebase = true;
    process.argv.splice(rebasePos, 1);
}

// Remove initial args
process.argv.shift();
process.argv.shift();

// Walk up directories looking for a build of proj
let projDir = process.cwd();
while (projDir) {
    const path = Path.join(projDir, "proj");
    if (fs.existsSync(path)) {
        break;
    }
    projDir = Path.dirname(projDir);
}
if (!projDir) {
    throw "Error: proj build not found (won't use the one under /usr).";
}

// Modify the path so that we point to our build of proj
process.env.PATH = projDir + ":" + process.env.PATH;

async function getTestPaths(inputPaths) {
    let paths = [];
    for (const path of inputPaths) {
        let isDir = false;
        try {
            isDir = fs.lstatSync(path).isDirectory();
        }
        catch (e) {
        };
        if (isDir) {
            paths = paths.concat(await globby([
                `${path}/**/*.test`,
                `${path}/**/*.sh`,
                '!node_modules/**/*'
            ]));
        } else {
            paths.push(path);
        }
    }
    return paths;
}

async function main() {
    const testPaths = await getTestPaths(process.argv);

    let passes = 0;
    let failures = 0;
    let baselines = 0;
    testPaths.forEach(testPath => {
        console.log("Test: " + testPath);

        // Figure out paths
        const ext = Path.extname(testPath);
        const expOutPath = testPath.replace(ext, ".out.exp");
        const actOutPath = testPath.replace(ext, ".out.act");
        const expErrPath = testPath.replace(ext, ".err.exp");
        const absPath = Path.resolve(testPath);

        // Cleanup last actual if it exists
        if (fs.existsSync(actOutPath)) {
            fs.unlinkSync(actOutPath);
        }

        // Execute and capture stdout and stderr
        let proc;
        if (ext === ".sh") {
            proc = spawnSync(absPath,
                [
                ],
                {
                    cwd: Path.dirname(absPath)
                }
            );
        }
        else {
            proc = spawnSync("proj",
                [
                    `@${absPath}`
                ],
                {
                    cwd: Path.dirname(absPath)
                }
            );
        }
        const actOut = proc.stdout.toString() || "";
        const actErr = proc.stderr.toString() || "";

        // Check whether expected out and err exists
        let noExpOutPresent = false;
        let expOut = "";
        if (fs.existsSync(expOutPath)) {
            expOut = fs.readFileSync(expOutPath, "utf8");
        }
        else {
            noExpOutPresent = true;
        }
        let noExpErrPresent = false;
        let expErr = "";
        if (fs.existsSync(expErrPath)) {
            expErr = fs.readFileSync(expErrPath, "utf8");
        }
        else {
            noExpErrPresent = true;
        }

        // Decide whether to generate baseline based on whether
        // we are missing an expected file or user used --rebase.
        let rebaseOut = rebase;
        let rebaseErr = rebase;
        if (!actErr && !rebaseOut && noExpOutPresent) {
            console.error("  Missing expected output");
            rebaseOut = true;
        }
        else if (actErr && !expOut && !rebaseErr && noExpErrPresent) {
            console.error("  Missing expected error");
            rebaseErr = true;
        }

        // Generate baselines if needed
        if (!actErr && rebaseOut) {
            if (fs.existsSync(expErrPath)) {
                fs.unlinkSync(expErrPath);
            }
            console.log("  Rebaselining: " + expOutPath);
            fs.writeFileSync(expOutPath, actOut, "utf8");
            baselines++;
        }
        else if (actErr && rebaseErr) {
            console.log("  Rebaselining: " + expErrPath);
            fs.writeFileSync(expErrPath, actErr, "utf8");
            baselines++;
        }

        // Report the result. We write the actual output if a failure in matching output is reported.
        if (!rebaseErr && !rebaseOut) {
            if (actErr) {
                if (expErr) {
                    if (fs.existsSync(expOutPath)) {
                        // We got an error, and expect an error, so ensure no expected output.
                        fs.unlinkSync(expOutPath);
                    }
                    if (expErr !== actErr) {
                        console.error("  Failure:");
                        console.error("    Expected error: " + expErr);
                        console.error("    Actual error:   " + actErr);
                        failures++;
                    }
                    else {
                        passes++;
                    }
                }
                else {
                    console.error("  Failure:");
                    console.error("    Expected error: none");
                    console.error("    Actual error:   " + actErr);
                    failures++;
                }
            }
            else {
                if (expErr) {
                    fs.writeFileSync(actOutPath, actOut, "utf8");
                    console.error("  Failure:");
                    console.error("    Expected error: " + expErr);
                    console.error("    Actual output (no error):  " + actOutPath);
                    failures++;
                } else if (actOut !== expOut) {
                    fs.writeFileSync(actOutPath, actOut, "utf8");
                    console.error("  Failure:");
                    console.error(`    diff ${expOutPath} ${actOutPath}`);
                    failures++;
                } else {
                    passes++;
                }
            }
        }
    });

    console.log("Passes: " + passes);
    console.log("Failures: " + failures);
    console.log("Baselines set: " + baselines);
}

// If no args, assume we search for test cases starting here.
if (process.argv.length == 0) {
    process.argv = ["."];
}

// Run
main(process.argv);

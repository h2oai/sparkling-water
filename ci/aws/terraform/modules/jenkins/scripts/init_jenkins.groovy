import jenkins.model.Jenkins
def instance = Jenkins.getInstance()

//
// Security settings
//
import hudson.security.FullControlOnceLoggedInAuthorizationStrategy
import hudson.security.HudsonPrivateSecurityRealm
import hudson.security.csrf.DefaultCrumbIssuer
import jenkins.security.s2m.AdminWhitelistRule
import jenkins.model.JenkinsLocationConfiguration

// Set Admin User
instance.setSecurityRealm(new HudsonPrivateSecurityRealm(false))
def user = instance.getSecurityRealm().createAccount('SUBST_JENKINS_USER', 'SUBST_JENKINS_PASS')
user.save()
def strategy = new FullControlOnceLoggedInAuthorizationStrategy()
strategy.setAllowAnonymousRead(false)
instance.setAuthorizationStrategy(strategy)


// Set Root URL
jlc = JenkinsLocationConfiguration.get()
jlc.setUrl("https://SUBST_PUBLIC_HOSTNAME")
jlc.save()

// Set Additional Security Options
instance.injector.getInstance(AdminWhitelistRule.class).setMasterKillSwitch(false)
instance.setNoUsageStatistics(true)
instance.setDisableRememberMe(true)
instance.setCrumbIssuer(new DefaultCrumbIssuer(true))
instance.save()


//
// Set Credentials
//
import hudson.util.Secret
import com.cloudbees.plugins.credentials.*
import com.cloudbees.plugins.credentials.domains.*
import com.cloudbees.jenkins.plugins.sshcredentials.impl.BasicSSHUserPrivateKey.FileOnMasterPrivateKeySource
import com.cloudbees.jenkins.plugins.sshcredentials.impl.*
import org.jenkinsci.plugins.plaincredentials.*
import org.jenkinsci.plugins.plaincredentials.impl.*
import com.cloudbees.jenkins.plugins.awscredentials.impl.*
import com.cloudbees.jenkins.plugins.awscredentials.*
import com.cloudbees.plugins.credentials.common.*
import com.cloudbees.plugins.credentials.impl.*
import com.cloudbees.jenkins.plugins.awscredentials.*
import java.nio.file.Files
import java.nio.file.Paths

def domain = Domain.global()
def store = instance.getExtensionList('com.cloudbees.plugins.credentials.SystemCredentialsProvider')[0].getStore()

def gitPrivateKey = new BasicSSHUserPrivateKey(
        CredentialsScope.GLOBAL,
        'h2oOpsGitPrivateKey',
        'SUBST_GITHUB_SSH_USER',
        new FileOnMasterPrivateKeySource("/home/ubuntu/.init/git_private_key.pem"),
        '',
        "SSH user and key to access Github"
)
store.addCredentials(domain, gitPrivateKey)

def awsPrivateKey = new BasicSSHUserPrivateKey(
        CredentialsScope.GLOBAL,
        'SW_AWS_PRIVATE_KEY',
        '',
        new FileOnMasterPrivateKeySource("/home/ubuntu/.init/aws_private_key.pem"),
        '',
        "Private key for accessing AWS"
)
store.addCredentials(domain, awsPrivateKey)

def awsCredentials = new AWSCredentialsImpl(
        CredentialsScope.GLOBAL,
        'SW_FULL_AWS_CREDS',
        'SUBST_AWS_ACCESS_KEY_ID',
        'SUBST_AWS_SECRET_ACCESS_KEY',
        'AWS Credentials',
        null,
        null
)
store.addCredentials(domain, awsCredentials)

def gitToken = new StringCredentialsImpl(
        CredentialsScope.GLOBAL,
        'SW_GITHUB_TOKEN',
        'GitHub Token',
        Secret.fromString('SUBST_GITHUB_TOKEN')
)
store.addCredentials(domain, gitToken)

def daiLicenseKey = new StringCredentialsImpl(
        CredentialsScope.GLOBAL,
        'DRIVERLESS_AI_LICENSE_KEY',
        'DAI License Key',
        Secret.fromString('SUBST_DAI_LICENSE_KEY')
)
store.addCredentials(domain, daiLicenseKey)

def signingKey = new UsernamePasswordCredentialsImpl(
        CredentialsScope.GLOBAL,
        'SIGNING_KEY',
        'Credentials used to sign artifacts',
        'SUBST_SIGN_KEY',
        ''
)
store.addCredentials(domain, signingKey)

def githubPull = new UsernamePasswordCredentialsImpl(
        CredentialsScope.GLOBAL,
        'GITHUB_PULL',
        'Github pull Credentials',
        'SUBST_GITHUB_PULL_USER',
        'SUBST_GITHUB_PULL_PASS'
)
store.addCredentials(domain, githubPull)

def signingFile = new FileCredentialsImpl(
        CredentialsScope.GLOBAL,
        'release-secret-key-ring-file',
        'Sining File for artifacts',
        'secring.gpg',
        SecretBytes.fromBytes(Files.readAllBytes(Paths.get('/home/ubuntu/.init/secring.gpg')))
)
store.addCredentials(domain, signingFile)

def nexus = new UsernamePasswordCredentialsImpl(
        CredentialsScope.GLOBAL,
        'PUBLIC_NEXUS',
        'Public Nexus Credentials',
        'SUBST_NEXUS_USERNAME',
        'SUBST_NEXUS_PASSWORD'
)
store.addCredentials(domain, nexus)

def dockerHub = new UsernamePasswordCredentialsImpl(
        CredentialsScope.GLOBAL,
        'dockerhub',
        'Docker Hub Credentials',
        'SUBST_DOCKERHUB_USERNAME',
        'SUBST_DOCKERHUB_PASSWORD'
)
store.addCredentials(domain, dockerHub)

def pipy = new UsernamePasswordCredentialsImpl(
        CredentialsScope.GLOBAL,
        'pypi-credentials',
        'Public Nexus Credentials',
        'SUBST_PIPY_USERNAME',
        'SUBST_PIPY_PASSWORD'
)
store.addCredentials(domain, pipy)

def conda = new UsernamePasswordCredentialsImpl(
        CredentialsScope.GLOBAL,
        'anaconda-credentials',
        'Public Nexus Credentials',
        'SUBST_ANACONDA_USERNAME',
        'SUBST_ANACONDA_PASSWORD'
)
store.addCredentials(domain, conda)

instance.save()


//
// Configure Shared Pipeline Library
//
import jenkins.plugins.git.GitSCMSource
import jenkins.plugins.git.traits.BranchDiscoveryTrait
import org.jenkinsci.plugins.workflow.libs.*
List libraries = [] as ArrayList

def remote = "https://github.com/h2oai/h2o-jenkins-pipeline-lib"
def credentialsId = "h2oOpsGitPrivateKey"
def name = 'test-shared-library'
def defaultVersion = 'master'

def scm = new GitSCMSource(remote)
if (credentialsId != null) {
    scm.credentialsId = credentialsId
}

scm.traits = [new BranchDiscoveryTrait()]
def retriever = new SCMSourceRetriever(scm)

def library = new LibraryConfiguration(name, retriever)
library.defaultVersion = defaultVersion
library.implicit = false
library.allowVersionOverride = true
library.includeInChangesets = false

libraries << library

def global_settings = Jenkins.instance.getExtensionList(GlobalLibraries.class)[0]
global_settings.libraries = libraries
global_settings.save()

//
// Configure EC2 plugin so we can use EC2 machines as slaves
//
import hudson.model.*
import jenkins.model.*
import hudson.plugins.ec2.*
import com.amazonaws.services.ec2.model.InstanceType
import hudson.plugins.ec2.EC2Tag

def getPrivateKey() {
    def cred = com.cloudbees.plugins.credentials.CredentialsProvider.lookupCredentials(
            com.cloudbees.plugins.credentials.Credentials.class,
            Jenkins.instance,
            null,
            null
    ).find{it.id == "SW_AWS_PRIVATE_KEY"}

    return cred.getPrivateKey()
}

def mac = "http://169.254.169.254/latest/meta-data/network/interfaces/macs/".toURL().text
def subnetId = "http://169.254.169.254/latest/meta-data/network/interfaces/macs/${mac}/subnet-id".toURL().text
def securityGroup = "http://169.254.169.254/latest/meta-data/network/interfaces/macs/${mac}/security-group-ids".toURL().text
def ami = new SlaveTemplate(
        'SUBST_AMI_ID', // ami
        'us-west-2b', // zone
        null, // spot configuration
        securityGroup, // securityGroups
        '/home/jenkins', // remoteFS
        InstanceType.fromValue('t2.xlarge'), // InstanceType type
        false, // ebsOptimized
        'docker', // labelString
        Node.Mode.NORMAL, // Node.Mode mode
        'worker_jenkins', // description
        '', // initScript
        '', // tmpDir
        '#!/bin/sh\nsudo cp -R /home/ec2-user/.ssh /home/jenkins\nsudo chown -R jenkins /home/jenkins\nsudo yum -y update --security', // userData
        '1', // numExecutors
        'jenkins', // remoteAdmin
        new UnixData('', '', '', '22'), // amiType
        '', // jvmopts
        false, // stopOnTerminate
        subnetId, // subnetId
        [new EC2Tag('Name', 'SW-Tests-Jenkins-Slave')], //tags
        '30', // idleTerminationMinutes
        0, // minimumNumberOfInstance
        0, // minimumNumberOfSpareInstances
        '20', // instanceCapStr
        '', // iamInstanceProfile
        false, // deleteRootOnTermination
        false, // useEphemeralDevices
        false, // useDedicatedTenancy
        '', // launchTimeoutStr
        true, // associatePublicIp
        '/dev/xvda=:60', // customDeviceMapping
        true, // connectBySSHProcess
        false, // monitoring
        false, // t2Unlimited
        hudson.plugins.ec2.ConnectionStrategy.PUBLIC_IP, // connectionStrategy
        -1, // maxTotalUses
        [] // node properties
)

def cloud = new AmazonEC2Cloud(
        'SparklingWaterInfra',
        false,
        'SW_FULL_AWS_CREDS',
        'us-west-2',
        getPrivateKey(),
        null,
        [ami],
        '',
        ''
)

instance.clouds.add(cloud)

//
// Add Sparkling Water Jobs
//
import hudson.util.*
import com.cloudbees.hudson.plugins.folder.*
import jenkins.branch.*
import jenkins.model.Jenkins
import jenkins.plugins.git.traits.LocalBranchTrait
import jenkins.scm.impl.trait.*
import org.jenkinsci.plugins.github_branch_source.*
import org.jenkinsci.plugins.workflow.multibranch.*
import jenkins.branch.buildstrategies.basic.*

def createJob(jobName, jobScript, includeRegex, suppressAutoTrigger) {
    Jenkins jenkins = Jenkins.instance
    WorkflowMultiBranchProject mbp = jenkins.createProject(WorkflowMultiBranchProject.class, jobName)
    mbp.getProjectFactory().setScriptPath(jobScript)
    GitHubSCMSource gitSCMSource = new GitHubSCMSource(null, null, "https://github.com/h2oai/sparkling-water", true)
    gitSCMSource.setCredentialsId("GITHUB_PULL")
    BranchSource branchSource = new BranchSource(gitSCMSource)

    BranchProperty[] bpa
    if (suppressAutoTrigger) {
        bpa = [new NoTriggerBranchProperty()]
    } else {
        bpa = null
    }
    branchSource.setStrategy(new DefaultBranchPropertyStrategy(bpa))
    branchSource.setBuildStrategies([new SkipInitialBuildOnFirstBranchIndexing()])
    def traits = []
    traits.add(new org.jenkinsci.plugins.github_branch_source.BranchDiscoveryTrait(3))
    traits.add(new OriginPullRequestDiscoveryTrait(2))
    traits.add(new ForkPullRequestDiscoveryTrait(1, new ForkPullRequestDiscoveryTrait.TrustContributors()))
    traits.add(new WildcardSCMHeadFilterTrait(includeRegex, ""))
    traits.add(new LocalBranchTrait())

    gitSCMSource.setTraits(traits)

    PersistedList sources = mbp.getSourcesList()
    sources.clear()
    sources.add(branchSource)
}

createJob(
        "TESTS_H2O_BRANCH",
        "ci/Jenkinsfile-from-h2o-branch",
        "master rel-* PR*",
        false)

createJob(
        "TESTS_PRE_RELEASE",
        "ci/Jenkinsfile",
        "rel-*",
        true)

createJob(
        "NIGHTLY_H2O_BRANCH",
        "ci/Jenkinsfile-nightly-from-h2o-branch",
        "master rel-*",
        true)

createJob(
        "BENCHMARKS",
        "ci/Jenkinsfile-benchmarks",
        "master rel-*",
        true)

createJob(
        "RELEASE",
        "ci/Jenkinsfile-release",
        "rel-*",
        true)

createJob(
        "TESTS_KUBERNETES",
        "ci/Jenkinsfile-kubernetes",
        "master rel-*",
        true)

// Scan repositories
for (f in Jenkins.instance.getAllItems(jenkins.branch.MultiBranchProject.class)) {
    f.computation.run()
}

//
// Register Hooks for github
//
import org.jenkinsci.plugins.github.config.GitHubPluginConfig
import org.jenkinsci.plugins.github.config.GitHubServerConfig


def server = new GitHubServerConfig('SW_GITHUB_TOKEN')
server.name = "Github Server"
server.apiUrl = 'https://api.github.com'
server.manageHooks = true
server.clientCacheSize = 20
def settings = Jenkins.instance.getExtensionList(GitHubPluginConfig.class)[0]
settings.configs = [server]
settings.save()
settings.doReRegister()

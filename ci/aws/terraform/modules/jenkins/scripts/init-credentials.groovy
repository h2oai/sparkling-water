import com.cloudbees.plugins.credentials.*
import com.cloudbees.plugins.credentials.common.*
import com.cloudbees.plugins.credentials.domains.*
import com.cloudbees.plugins.credentials.impl.*
import hudson.util.Secret
import jenkins.model.*
import jenkins.model.Jenkins
import org.jenkinsci.plugins.plaincredentials.*
import org.jenkinsci.plugins.plaincredentials.impl.*

Jenkins jenkins = Jenkins.getInstance()
def domain = Domain.global()
def store = jenkins.getExtensionList('com.cloudbees.plugins.credentials.SystemCredentialsProvider')[0].getStore()

def gitToken = new StringCredentialsImpl(
        CredentialsScope.GLOBAL,
        'h2o-ops-personal-auth-token',
        'GitHub Token',
        Secret.fromString('SUBST_GITHUB_TOKEN')
)

def daiLicenseKey = new StringCredentialsImpl(
        CredentialsScope.GLOBAL,
        'DRIVERLESS_AI_LICENSE_KEY',
        'DAI License Key',
        Secret.fromString('SUBST_DAI_LICENSE_KEY')
)

def signingKey = new UsernamePasswordCredentialsImpl(
        CredentialsScope.GLOBAL,
        'SIGNING_KEY',
        'Credentials used to sign artifacts',
        'SUBST_SIGN_KEY',
        ''
)

def githubPull = new UsernamePasswordCredentialsImpl(
        CredentialsScope.GLOBAL,
        'GITHUB_PULL',
        'Github pull Credentials',
        'SUBST_GITHUB_PULL_USER',
        'SUBST_GITHUB_PULL_PASS'
)

def createSecretBytes(path) {
    try (FileInputStream fis = new FileInputStream(path)) {
        return  SecretBytes.fromBytes(fis.readAllBytes())
    } catch (Exception e) {
        e.printStackTrace();
    }
}
def signingFile = new FileCredentialsImpl(
        CredentialsScope.GLOBAL,
        'release-secret-key-ring-file',
        'Sining File for artifacts',
        'secring.gpg',
        createSecretBytes('/home/jenkins/.init/secring.gpg')
)

store.addCredentials(domain, gitToken)
store.addCredentials(domain, daiLicenseKey)
store.addCredentials(domain, signingKey)
store.addCredentials(domain, githubPull)
store.addCredentials(domain, signingFile)

jenkins.save()

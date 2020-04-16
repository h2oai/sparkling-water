import hudson.security.*
import hudson.security.csrf.DefaultCrumbIssuer
import jenkins.model.*
import jenkins.security.QueueItemAuthenticatorConfiguration
import jenkins.security.s2m.*
import org.jenkinsci.plugins.authorizeproject.GlobalQueueItemAuthenticator
import org.jenkinsci.plugins.authorizeproject.strategy.SystemAuthorizationStrategy

// Disable Master Access Control
Jenkins.instance.injector.getInstance(AdminWhitelistRule.class).setMasterKillSwitch(false)
Jenkins.instance.save()

// Set Root URL
jlc = JenkinsLocationConfiguration.get()
jlc.setUrl("https://SUBST_PUBLIC_HOSTNAME")
jlc.save()

def instance = Jenkins.getInstance()
instance.setAuthorizationStrategy(new FullControlOnceLoggedInAuthorizationStrategy())
instance.setNoUsageStatistics(true)
instance.setDisableRememberMe(true)
instance.setSecurityRealm(new HudsonPrivateSecurityRealm(false))
instance.setAuthorizationStrategy(new GlobalMatrixAuthorizationStrategy())
instance.setCrumbIssuer(new DefaultCrumbIssuer(true))

// Configure Authorization
GlobalQueueItemAuthenticator auth = new GlobalQueueItemAuthenticator(
        new SystemAuthorizationStrategy()
)
QueueItemAuthenticatorConfiguration.get().authenticators.add(auth)

def user = instance.getSecurityRealm().createAccount('SUBST_JENKINS_USER', 'SUBST_JENKINS_PASS')
user.save()
instance.getAuthorizationStrategy().add(Jenkins.ADMINISTER, 'SUBST_JENKINS_USER')

instance.save()

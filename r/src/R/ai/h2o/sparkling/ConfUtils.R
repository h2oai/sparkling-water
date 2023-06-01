ConfUtils.getOption <- function(option) {
    if (invoke(option, "isDefined")) {
        invoke(option, "get")
    } else {
        NA_character_
    }
}

#' @export
runExample <- function() {
  appDir <- system.file("myapp", package = "getPBFData")
  if (appDir == "") {
    stop("Could not find myapp. Try re-installing `mypackage`.", call. = FALSE)
  }

  shiny::runApp(appDir, display.mode = "normal")
}

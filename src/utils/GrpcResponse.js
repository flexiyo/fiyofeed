export class GrpcResponse {
  static success(message = "Success") {
    return { status: { success: true, message } };
  }

  static error(message = "An internal error occurred") {
    return { status: { success: false, message } };
  }
}
